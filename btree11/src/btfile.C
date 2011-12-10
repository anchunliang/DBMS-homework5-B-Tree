/*
 * btfile.C - function members of class BTreeFile
 *
 * Johannes Gehrke & Gideon Glass  951022  CS564  UW-Madison
 */

#include <iostream>

#include "minirel.h"
#include "buf.h"
#include "db.h"
#include "new_error.h"
#include "btree_file_scan.h"
#include "btfile.h"

const int MAGIC0 = 0xfeeb1e;

/*
 * NOTE: (on error handling)  We use the `assert' macro to check the
 * consistency of our internal data structures and variables, etc.  We
 * do not call MINIBASE_xxx_ERROR on these because failed asserts
 * would be due to bugs in our code and need to be flagged immediately (ie
 * runtime assert failed message) instead of propagating upwards as normal
 * errors do.  Failed asserts are not due to normal causes of db failure.
 */


const char* BTreeFile::errors[BTreeFile::NR_ERRORS] = {
	"No error---this is for `OK'",              // _OK
	"db can't find header page",                // CANT_FIND_HEADER
	"buffer manager failed to pin header page", // CANT_PIN_HEADER,
	"failed to allocate block for header page", // CANT_ALLOC_HEADER
	"buffer manager failed to allocate block for header page", // CANT_ALLOCK_BLOCK
	"couldn't register new index file w/ db",   // CANT_ADD_FILE_ENTRY
	"can't unpin header page",                  // CANT_UNPIN_HEADER
	"can't pin index/leaf page",                // CANT_PIN_PAGE
	"can't unpin index/leaf page",              // CANT_UNPIN_PAGE
	"attempt to use invalid scan object",       // INVALID_SCAN
	"SortedPage failed to delete current rid",  // DELETE_CURRENT_FAILED
	"db failed to delete file entry",           // CANT_DELETE_FILE_ENTRY
	"buffer manager failed to free a page",     // CANT_FREE_PAGE,
	"_destroyFile failed on a subtree",         // CANT_DELETE_SUBTREE,
	"BTreeFile::insert : key too long",         // KEY_TOO_LONG
	"BtreeFile::insert : insert failed",        // INSERT_FAILED
	"BTreeFile::insert : could not create new root", // COULD_NOT_CREATE_ROOT
	"could not delete a data entry",            // DELETE_DATAENTRY_FAILED
	"could not find data entry to delete",      // DATA_ENTRY_NOT_FOUND
	"get_page_no on BTIndexPage failed",        // CANT_GET_PAGE_NO
	"bm::newPage failed",                       // CANT_ALLOCATE_NEW_PAGE
	"could not split leaf page",                // CANT_SPLIT_LEAF_PAGE
	"could not split index page"                // CANT_SPLIT_INDEX_PAGE
};


static ErrorStringTable btree_table( BTREE, BTreeFile::errors );
static ErrorStringTable btlp_table( BTLEAFPAGE, BTLeafPage::errors );
static ErrorStringTable btip_table( BTINDEXPAGE, BTIndexPage::errors );
static ErrorStringTable sp_table( SORTEDPAGE, SortedPage::errors );


/*
 *  BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
 *
 *  an index with given filename should already exist,
 *  this opens it.
 */

BTreeFile::BTreeFile (Status& returnStatus, const char *filename)
{
	Status st;

	st = MINIBASE_DB->get_file_entry(filename, headerPageId);
	if (st != OK) {
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_FIND_HEADER);
		return;
	}

	st = MINIBASE_BM->pinPage(headerPageId, (Page *&) headerPage);
	if (st != OK) {
		returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);
		return;
	}

	dbname = strcpy(new char[strlen(filename)+1],filename);

	assert(headerPage->magic0 == (unsigned)MAGIC0);

	// ASSERTIONS:
	/*
	 *
	 * - headerPageId is the PageId of this BTreeFile's header page;
	 * - headerPage, headerPageId valid and pinned
	 * - dbname contains a copy of the name of the database
	 */

	returnStatus = OK;
}

/*
 * BTreeFile::BTreeFile (Status& returnStatus, const char *filename,
 *                      const AttrType keytype, const int keysize)
 *
 * Open B+ tree index, creating w/ specified keytype and size if necessary.
 */

BTreeFile::BTreeFile (Status& returnStatus, const char *filename,
		const AttrType keytype,
		const int keysize, int delete_fashion)
{
	Status st;

	st = MINIBASE_DB->get_file_entry(filename, headerPageId);
	if (st != OK) {
		// create new BTreeFile; first, get a header page.
		st = MINIBASE_BM->newPage(headerPageId, (Page *&) headerPage);
		if (st != OK) {
			headerPageId = INVALID_PAGE;
			headerPage = NULL;
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_ALLOC_HEADER);
			return;
		}

		// now add btreefile to the naming service of the database
		st = MINIBASE_DB->add_file_entry(filename, headerPageId);
		if (st != OK) {
			headerPageId = INVALID_PAGE;
			headerPage = NULL;
			MINIBASE_BM->freePage(headerPageId);
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_ADD_FILE_ENTRY);
			return;
		}

		// initialize headerpage; we actually never reference
		// prevPage or nextPage, though.


		((HFPage*) headerPage)->init(headerPageId);
		((HFPage*) headerPage)->setNextPage(INVALID_PAGE);
		((HFPage*) headerPage)->setPrevPage(INVALID_PAGE);
		((SortedPage*) headerPage)->set_type(LEAF);

		headerPage->magic0 = MAGIC0;
		headerPage->root = INVALID_PAGE;
		headerPage->key_type = keytype;
		headerPage->keysize = keysize;
		headerPage->delete_fashion = delete_fashion;


	} else {
		// open an existing btreefile

		st = MINIBASE_BM->pinPage(headerPageId, (Page *&) headerPage);
		if (returnStatus != OK) {
			returnStatus = MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);
			return;
		}
		assert(headerPage->magic0 == (unsigned)MAGIC0);
	}

	dbname = strcpy(new char[strlen(filename)+1],filename);


	// ASSERTIONS:
	/*
	 * - headerPageId is the PageId of this BTreeFile's header page;
	 * - headerPage points to the pinned header page (headerPageId)
	 * - dbname contains the name of the database
	 */

	returnStatus = OK;
}

/*
 * BTreeFile::~BTreeFile ()
 *
 * minor cleanup work.  Unpin headerPageId if necessary.
 * (It may have been blown away by a destroyFile() previously.)
 */

BTreeFile::~BTreeFile ()
{
	delete [] dbname;

	if (headerPageId != INVALID_PAGE) {
		Status st = MINIBASE_BM->unpinPage(headerPageId);
		if (st != OK)
			MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	}
}


/*
 * Status BTreeFile::updateHeader (PageId newRoot)
 *
 * Change root of B+ tree to specified new root.
 *
 * Modifies the header page and feeds the dirty bit to buffer manager.
 */

Status BTreeFile::updateHeader (PageId newRoot)
{
	Status st;
	BTreeHeaderPage *pheader;
	PageId old_data;

	st = MINIBASE_BM->pinPage(headerPageId, (Page *&) pheader);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_HEADER);

	old_data = pheader->root;
	pheader->root = newRoot;


	// clock in dirty bit to bm so our dtor needn't have to worry about it
	st = MINIBASE_BM->unpinPage(headerPageId, 1 /* = DIRTY */ );
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_HEADER);

	// ASSERTIONS:
	// - headerPage, headerPageId valid, pinned and marked as dirty

	return OK;
}

/*
 *  Status BTreeFile::destroyFile ()
 *
 * Destroy entire index file.
 *
 * Most work done recursively by _destroyFile().
 */

Status BTreeFile::destroyFile ()
{
	Status st;

	if (headerPage->root != INVALID_PAGE) {
		// if tree non-empty
		st = _destroyFile(headerPage->root);
		if (st != OK) return st; // if it encountered an error, it would've added it
	}

	st = MINIBASE_BM->unpinPage(headerPageId);
	if (st != OK)
		MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);

	PageId hdrId = headerPageId;        // Deal with the possibility
	// that the freePage might fail.
	headerPageId = INVALID_PAGE;
	headerPage   = NULL;

	st = MINIBASE_BM->freePage(hdrId);
	if (st != OK)
		MINIBASE_FIRST_ERROR(BTREE, CANT_FREE_PAGE);

	st = MINIBASE_DB->delete_file_entry(dbname);
	if (st != OK)
		MINIBASE_FIRST_ERROR(BTREE, CANT_DELETE_FILE_ENTRY);

	// the destructor will take care of freeing the memory at dbname

	return OK;
}

/*
 * Status BTreeFile::_destroyFile (PageId pageno)
 *
 * Recursively free all nodes of a B+ tree starting at specified
 * page, which is freed last of all.
 */

Status BTreeFile::_destroyFile (PageId pageno)
{
	Status st;
	SortedPage *pagep;

	st = MINIBASE_BM->pinPage(pageno, (Page *&) pagep);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);


	NodeType ndtype = pagep->get_type();
	if (ndtype == INDEX) {
		RID    rid;
		PageId childId;
		BTIndexPage* ipagep = (BTIndexPage *) pagep;

		for (st = ipagep->get_first(rid, NULL, childId);
				st != NOMORERECS;
				st = ipagep->get_next(rid, NULL, childId)) {

			Status tmpst = _destroyFile(childId);

			if (tmpst != OK) {
				MINIBASE_FIRST_ERROR(BTREE, CANT_DELETE_SUBTREE);
			}
		}
	} else {
		assert(ndtype == LEAF);
	}

	// ASSERTIONS:
	// - if pagetype == INDEX: the subtree rooted at pageno is completely
	//                         destroyed

	st = MINIBASE_BM->unpinPage(pageno);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	st = MINIBASE_BM->freePage(pageno);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_FREE_PAGE);

	// ASSERTIONS:
	// - pageno invalid and set free

	return OK;
}

/*
 * Status BTreeFile::insert (const void *key, const RID rid)
 *
 * insert recid with the key.
 *
 * (`recid' is an opaque RID specified by the user; we don't look
 * at its contents at all.)
 *
 * Most work done recursively by _insert, which propogates up a new
 * root if the old root happened to split.
 *
 * Special case: create root if it previously didn't exist (i.e., the
 * index had no entries).
 */

Status BTreeFile::insert (const void *key, const RID rid)
{
	Status returnStatus;
	KeyDataEntry  newRootEntry;
	int           newRootEntrySize;
	KeyDataEntry* newRootEntryPtr = &newRootEntry;

	if (get_key_length(key, headerPage->key_type) > headerPage->keysize)
			return MINIBASE_FIRST_ERROR(BTREE, KEY_TOO_LONG);

	// TWO CASES:
	// 1. headerPage->root == INVALID_PAGE:
	//    - the tree is empty and we have to create a new first page;
	//      this page will be a leaf page
	// 2. headerPage->root != INVALID_PAGE:
	//    - we call _insert() to insert the pair (key, rid)


	if (headerPage->root == INVALID_PAGE) {
			
		// TODO: fill the body
		PageId rootPageId = -1;
		BTLeafPage* rootLeafPage = new BTLeafPage();
		Status st = MINIBASE_BM->new_page( (PageId&)rootPageId, (BTLeafPage*&)rootLeafPage );
		assert( st == OK);
		assert( rootPageId != -1);
		rootLeafPage->init( rootPageId);
		headerPage->root =  rootPageId;
		//		return OK;
	}

	returnStatus = _insert(key, rid, &newRootEntryPtr, &newRootEntrySize, headerPage->root);

	if (returnStatus != OK)
			MINIBASE_FIRST_ERROR(BTREE, INSERT_FAILED);

	// TWO CASES:
	// - newRootEntryPtr != NULL: a leaf split propagated up to the root
	//                                and the root split: the new pageNo is in
	//                            newChildEntry->data->pageNo
	// - newRootEntryPtr == NULL: no new root was created;
	//                            information on headerpage is still valid


	if (newRootEntryPtr != NULL) {
			// TODO: fill the body
			headerPage->root = newRootEntryPtr->data->pageNo;

	}

	return OK;
}

/*
 *
 * Status BTreeFile::_insert (const void    *key,
 *                            const RID     rid,
 *                            KeyDataEntry  **goingUp,
 *                            int           *goingUpSize,
 *                            PageId        currentPageId)
 *
 * Do a recursive B+ tree insert of data entry <key, rid> into tree rooted
 * at page currentPageId.
 *
 * If this page splits, copy (if we're on a leaf) or push (if on an index page)
 * middle entry up by setting *goingUp to it.  Otherwise (no split) set
 * *goingUp to NULL.
 *
 * Code is long, but fairly straighforward.  Two big cases for INDEX and LEAF
 * pages.  (We use a switch for clarity, not because we expect more
 * page types to appear.)
 */

Status BTreeFile::_insert (const void *key, const RID rid,
		KeyDataEntry **goingUp, int *goingUpSize, PageId currentPageId)

{
	Status st;
	SortedPage* rpPtr;

	assert(currentPageId != INVALID_PAGE);
	assert(*goingUp != NULL);

	st = MINIBASE_BM->pinPage(currentPageId,(Page *&) rpPtr);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);


	NodeType pageType = rpPtr->get_type();


	// TWO CASES:
	// - pageType == INDEX:
	//   recurse and then split if necessary
	// - pageType == LEAF:
	//   try to insert pair (key, rid), maybe split

	switch (pageType)
	{
		case INDEX:
		{
			// two cases:
			// - *goingUp == NULL: one level lower no split has occurred:
			//                     we are done.
			// - *goingUp != NULL: one of the children has split and
			//                     **goingUp is the new data entry which has
			//                    to be inserted on this index page

			
			// TODO: fill the body
						
			if( *goingUp != NULL){
				
			}

			break;
		}

		case LEAF:
		{

			// check whether there is duplicate key
			// if yes, print out "Duplicate" and return OK
			// if no, continue
			
			// TODO: fill the body
			
			break;
		}

		default:        // in case memory is scribbled upon & type is hosed
			assert(false);
	}

	return OK;
}

/*
 *  Status BTreeFile::Delete (const void *key, const RID rid)
 *
 * Remove specified data entry (<key, rid>) from an index.
 * Based on the value of headerPage->delete_fashion to use either naive delete
 * algorithm or full delete algorithm (involving merge & redistribution)
 */
Status BTreeFile::Delete(const void *key, const RID rid)
{
	if (headerPage->delete_fashion == FULL_DELETE)
		return fullDelete(key, rid);
	else {
		// headerPage->delete_fashion == NAIVE_DELETE
		return naiveDelete(key, rid);
	}
}

/*
 *  Status BTreeFile::naiveDelete (const void *key, const RID rid)
 *
 * Remove specified data entry (<key, rid>) from an index.
 *
 *
 * Page containing first occurrence of key `key' is found for us
 * by findRunStart.  We then iterate for (just a few) pages, if necesary,
 * to find the one containing <key,rid>, which we then delete via
 * BTLeafPage::delUserRid.
 */

Status BTreeFile::naiveDelete (const void *key, const RID rid)
{
	BTLeafPage *leafp;
	RID curRid;  // iterator
	Status st;
	Keytype curkey;
	RID dummyRid;
	PageId nextpage;
	bool deleted;

#ifdef BT_TRACE
	cerr << "DELETE " << rid.pageNo << " " << rid.slotNo << " " << (char*)key << endl;
	cerr << "DO" << endl;
	cerr << "SEARCH" << endl;
#endif

	st = findRunStart(key, &leafp, &curRid);  // find first page,rid of key
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, DELETE_DATAENTRY_FAILED);


	leafp->get_current(curRid, &curkey, dummyRid);
	while (keyCompare(key, &curkey, headerPage->key_type) == 0) {


		deleted = leafp->delUserRid(key, headerPage->key_type, rid);
		if (deleted) {
			// successfully found <key, rid> on this page and deleted it.
			// unpin dirty page and return OK.

			st = MINIBASE_BM->unpinPage(leafp->page_no(), TRUE /* = DIRTY */);
			if (st != OK) {
				MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
				return MINIBASE_FIRST_ERROR(BTREE, DELETE_DATAENTRY_FAILED);
			}

#ifdef BT_TRACE
	cerr << "TAKEFROM node " << leafp->page_no() << endl;
	cerr << "DONE" << endl;
#endif
			return OK;
		}

		nextpage = leafp->getNextPage();
		st = MINIBASE_BM->unpinPage(leafp->page_no());
		if (st != OK) {
			MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
			return MINIBASE_FIRST_ERROR(BTREE, DELETE_DATAENTRY_FAILED);
		}

		st = MINIBASE_BM->pinPage(nextpage, (Page *&) leafp);
		if (st != OK) {
			MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);
			return MINIBASE_FIRST_ERROR(BTREE, DELETE_DATAENTRY_FAILED);
		}

		leafp->get_first(curRid, &curkey, dummyRid);
	}

	/*
	 * We reached a page with first key > `key', so return an error.
	 * We should have got true back from delUserRid above.  Apparently
	 * the specified <key,rid> data entry does not exist.
	 */

	st = MINIBASE_BM->unpinPage(leafp->page_no());
	if (st != OK)
		MINIBASE_FIRST_ERROR(BTREE, CANT_UNPIN_PAGE);
	return MINIBASE_FIRST_ERROR(BTREE, DELETE_DATAENTRY_FAILED);
}

/*
 * Status BTreeFile::fullDelete (const void *key, const RID rid)
 * 
 * Remove specified data entry (<key, rid>) from an index.
 *
 * Most work done recursively by _delete
 *
 * Special case: delete root if the tree is empty
 *
 * Page containing first occurrence of key `key' is found for us
 * After the page containing first occurence of key 'key' is found,
 * we iterate for (just a few) pages, if necesary,
 * to find the one containing <key,rid>, which we then delete via
 * BTLeafPage::delUserRid.
 */

Status BTreeFile::fullDelete (const void *key, const RID rid)
{
	Keytype oldChildKey;
	void *oldChildKeyPtr = &oldChildKey;


#ifdef BT_TRACE
	cerr << "DELETE " << rid.pageNo << " " << rid.slotNo << " " << (char*)key << endl;
	cerr << "DO" << endl;
	cerr << "SEARCH" << endl;
#endif

	Status st = _delete(key, rid, oldChildKeyPtr, headerPage->root, -1);

#ifdef BT_TRACE
	cerr << "DONE\n";
#endif
	return st;
}

Status BTreeFile::_delete (const void    *key,
		const RID     rid,
		void          *&oldChildEntry,
		PageId        currentPageId,
		PageId        parentPageId)
{


	return OK;
}

/*
 * IndexFileScan* BTreeFile::new_scan (const void *lo_key, const void *hi_key)
 *
 * create a scan with given keys
 * Cases:
 *      (1) lo_key = NULL, hi_key = NULL
 *              scan the whole index
 *      (2) lo_key = NULL, hi_key!= NULL
 *              range scan from min to the hi_key
 *      (3) lo_key!= NULL, hi_key = NULL
 *              range scan from the lo_key to max
 *      (4) lo_key!= NULL, hi_key!= NULL, lo_key = hi_key
 *              exact match ( might not unique)
 *      (5) lo_key!= NULL, hi_key!= NULL, lo_key < hi_key
 *              range scan from lo_key to hi_key
 *
 * The work of finding the first page to scan is done by findRunStart (below).
 */

IndexFileScan *BTreeFile::new_scan(const void *lo_key, const void *hi_key)
{
	Status st;

	BTreeFileScan *scanp = new BTreeFileScan();

	if (headerPage->root == INVALID_PAGE) {
		// tree is empty, so return a scan object that will iterate zero times.
		scanp->leafp = NULL;
		return scanp;
	}

	scanp->treep = this;
	scanp->endkey = hi_key;  // may need to copy data over

	scanp->didfirst = false;
	scanp->deletedcurrent = false;

	// this sets up scanp at starting position, ready for iteration:
	st = findRunStart(lo_key, &scanp->leafp, &scanp->curRid);
	if (st != OK) {
		// error (if any) has already been registered by findScanStart
		scanp->leafp = NULL; // for ~BTreeFileScan
		delete scanp;
		return NULL;
	}

	return scanp;
}


/*
 * Status BTreeFile::findRunStart (const void   *lo_key,
 *                                BTLeafPage  **pppage,
 *                                RID          *pstartrid)
 *
 * find left-most occurrence of `lo_key', going all the way left if
 * lo_key is NULL.
 *
 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
 *
 */

Status BTreeFile::findRunStart (const void   *lo_key,
		BTLeafPage  **pppage,
		RID          *pstartrid)
{
	BTLeafPage *ppage;
	BTIndexPage *ppagei;
	PageId pageno;
	PageId curpage;                // iterator
	PageId prevpage;
	PageId nextpage;
	RID metaRid, curRid;
	Keytype curkey;
	Status st;
	AttrType key_type = headerPage->key_type;

	pageno = headerPage->root;
	if (pageno == INVALID_PAGE){        // no pages in the BTREE
		*pppage = NULL;                // should be handled by
		pstartrid = NULL;             // the caller
		return OK;
	}
	st = MINIBASE_BM->pinPage(pageno, (Page *&) ppagei);
	if (st != OK)
		return MINIBASE_FIRST_ERROR(BTREE, CANT_PIN_PAGE);

	while (ppagei->get_type() == INDEX) {

			// TODO: fill the body
			st = ppagei->get_first( metaRid, &curkey, curpage);
			assert( st ==OK);
			st = MINIBASE_BM->pinPage( curpage, (Page*&) ppagei);
			assert( st ==OK);
	}

	assert(ppagei);
	assert(ppagei->get_type() == LEAF);
	ppage = (BTLeafPage *) ppagei;

	st = ppage->get_first(metaRid, &curkey, curRid);

	while (st == NOMORERECS) {

			// TODO: fill the body

	}

	if (lo_key == NULL) {
		*pppage = ppage;
		*pstartrid = metaRid;
		return OK;
		// note that pageno/ppage is still pinned; scan will unpin it when done
	}

	while (keyCompare(&curkey, lo_key, key_type) < 0) {

			// TODO: fill the body
			st = ppage->get_next( metaRid, &curkey, curRid);
			assert( st == OK);
			st = MINIBASE_BM->pinPage( curRid, (Page*&) ppage);
			assert( st == OK);
	}


	*pppage = ppage;
	*pstartrid = metaRid;

	return OK;
}

int BTreeFile::keysize()
{
	return headerPage->keysize;
}




void BTreeFile::printHeader()
{
	cout << "\nPRINTING B-TREE HEADER PAGE-------------------------------\n";
	cout << "Header page info of BTree index " << dbname << " :" << endl;
	cout << "  Root page number : " << headerPage->root << endl;
	cout << "  Key type : ";
	switch (headerPage->key_type)
	{
		case attrInteger: cout << "Integer" << endl; break;
		case attrReal:    cout << "Real"    << endl; break;
		case attrString:  cout << "String"  << endl; break;
		default: break;
	}
	cout << "  Key size : " << headerPage->keysize << endl << endl;
}

void BTreeFile::printRoot()
{
	cout << "\nPRINTING B-TREE ROOT PAGE-------------------\n";
	if (headerPage->root != INVALID_PAGE)
		printPage( headerPage->root );
}

void BTreeFile::printLeafPages()
{
	Status st;
	BTLeafPage *leafp;
	RID dummy;


	// Find first leaf node.
	st = findRunStart( NULL, &leafp, &dummy );
	if ( st != OK )
	{
		cerr << "Error finding start of b-tree" << endl;
		return;
	}

	while ( leafp )
	{
		printPage( leafp->page_no() );

		PageId next = leafp->getNextPage();
		st = MINIBASE_BM->unpinPage( leafp->page_no() );
		if (st != OK)
		{
			MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_UNPIN_PAGE);
			cerr << "Can't unpin a b-tree page" << endl;
			return;
		}

		if ( next == INVALID_PAGE )
			leafp = NULL;
		else
		{
			st = MINIBASE_BM->pinPage( next, (Page *&) leafp );
			if (st != OK)
			{
				MINIBASE_FIRST_ERROR(BTREE, BTreeFile::CANT_PIN_PAGE);
				cerr << "Can't pin a b-tree page" << endl;
				return;
			}
		}
	}
}

void BTreeFile::printPage(PageId id)
{
	Status st;
	SortedPage* page;

	st = MINIBASE_BM->pinPage(id, (Page*&)page );
	if ( st != OK )
	{
		cerr << "Error reading b-tree page #" << id << endl;
		return;
	}

	cout << "\n------------------------------------------------------\n";
	cout << "B-Tree page #" << id << endl;
	cout << "Number of records : " << page->numberOfRecords() << endl;

	if ( page->get_type() == LEAF )
	{
		cout << "Node type : Leaf" << endl;
		cout << "Right sibling : " << page->getNextPage() << endl;
		cout << "--------------records in the page------------------" << endl;

		BTLeafPage* leafp = (BTLeafPage*) page;
		RID metaRid, dataRid;
		Keytype key;
		for ( st = leafp->get_first( metaRid, &key, dataRid );
				st == OK;
				st = leafp->get_next( metaRid, &key, dataRid ) )
		{
			cout << "Page/slot: " << dataRid.pageNo << '/'
				<< dataRid.slotNo << " Key: ";
			switch ( headerPage->key_type )
			{
				case attrString:    cout << key.charkey; break;
				case attrInteger:   cout << key.intkey;  break;
				default: break;
			}
			cout << endl;
		}
	}
	else if (page->get_type() == INDEX)
	{
		cout << "Node type : Internal" << endl;
		cout << "Left-most child : " << page->getPrevPage() << endl;
		cout << "--------------records in the page------------------" << endl;

		BTIndexPage *indexp = (BTIndexPage*) page;
		RID metaRid;
		PageId pg;
		Keytype key;
		for ( st = indexp->get_first( metaRid, &key, pg );
				st == OK;
				st = indexp->get_next( metaRid, &key, pg ) )
		{
			cout << "Page: " << pg << " Key: ";
			switch ( headerPage->key_type )
			{
				case attrString:    cout << key.charkey; break;
				case attrInteger:   cout << key.intkey;  break;
				default: break;
			}
			cout << endl;
		}
	}
	else
	{
		cout << "Internal ERROR in " << __FILE__ << " at " << __LINE__ << endl;
		cout << "Node type : Invalid." << endl;
		cout << "Page #" << id << " (" << page << ")" << endl;
	}

	cout << "------------------end of page--------------------------\n\n";
	MINIBASE_BM->unpinPage(id);
}


#if defined(BT_TRACE)


void BTreeFile::trace_children(PageId id)
{
	cerr << "CHILDREN " << id << " nodes" << endl;

	BTIndexPage* pg;
	RID metaRid;
	PageId childPageId;
	Keytype key;
	Status st;

	MINIBASE_BM->pinPage( id, (Page*&)pg );

	// Now print all the child nodes of the page.  (This is only called on
	// internal nodes.)
	cerr << ' ' << pg->getPrevPage();
	for ( st = pg->get_first( metaRid, &key, childPageId );
			st == OK;
			st = pg->get_next( metaRid, &key, childPageId ) )
	{
		cerr << ' ' << childPageId;
	}

	MINIBASE_BM->unpinPage( id );
	cerr << endl;
}



#endif
