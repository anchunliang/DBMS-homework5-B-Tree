#include "new_error.h"
#include "string.h"
#include "stdio.h"
#include "stdlib.h"


GlobalErrors minibase_errors;

/* Declare static variables or define methods of class ErrorStringTable */
const char** ErrorStringTable::table[NUM_STATUS_CODES];

const char* ErrorStringTable::get_message( Status subsystem, int index )
{
	const char** messages = table[subsystem];
	if ( messages != NULL && index >= 0 )
		return messages[index];
	else
		return NULL;
}



/* Define methods of class ErrorNode */
ErrorNode::ErrorNode( Status subsys, Status prior, int err_index,
                        const char* extra_msg )
{
	next_node = NULL;
	subsystem = subsys;
	prior_status = prior;

	msg = NULL;
	if (extra_msg != NULL) {
		msg = new char[strlen(extra_msg) + 1];
		strcpy(msg, extra_msg);
	}
	error_index = err_index;
}


ErrorNode::~ErrorNode()
{
	delete msg;
}


void ErrorNode::show_error( ostream& to ) const
{
	if ( prior_status == OK )
		to << team_name(subsystem);
	else
	{
		to << "--> " << team_name(subsystem);
		to << "[from the " << team_name(prior_status) << "]";
	}

	const char* index_msg = get_message();
	if ( index_msg )
		to << ": " << index_msg;
	if ( msg )
		to << ": " << msg;
	to << endl;
}

const char* ErrorNode::team_name(Status T1)
{
	switch (T1) {

		case BUFMGR:
			return "Buffer Manager";

		case BTREE:
			return "BTree";

		case SORTEDPAGE:
			return "Sorted Page";

		case BTINDEXPAGE:
			return "BTree Index Page";

		case BTLEAFPAGE:
			return "BTree Leaf Page";

		case JOINS:
			return "Joins";

		case PLANNER:
			return "Planner";

		case PARSER:
			return "Parser";

		case OPTIMIZER:
			return "Optimizer";

		case FRONTEND:
			return "Front End";

		case CATALOG:
			return "Catalog";

		case HEAPFILE:
			return "Heap File";

		case DBMGR:
			return "DB Manager";

		default:
			return "<<Unknown>>";
	}

	return NULL;
}


/* Define methods of class ErrorNode */
GlobalErrors::GlobalErrors()
{
	first = NULL;
	last = NULL;
}


Status GlobalErrors::add_error( ErrorNode* next )
{
	if (last != NULL)
		last->set_next(next);
	else
		first = next;
	last = next;
	return next->get_status();
}


Status GlobalErrors::add_error( Status subsystem, Status priorStatus,
                                 int lineno, const char *file, int error_index )
{
	char extra[strlen(file) + 10];
	sprintf( extra, "%s:%d", file, lineno );
	return add_error( new ErrorNode(subsystem, priorStatus, error_index,extra) );
}


void GlobalErrors::show_errors( ostream& to )
{
	if (first != NULL)
		to << "First error occurred: ";
	for ( const ErrorNode* err = first; err != NULL; err = err->get_next() )
		err->show_error(to);
}

void GlobalErrors::show_errors()
{
	show_errors( cerr );
}

void GlobalErrors::clear_errors()
{
	const ErrorNode *err = first;
	while (err != NULL)
	{
		const ErrorNode *prev = err;
		err = err->get_next();
		delete prev;
	}

	first = last = NULL;
}



GlobalErrors::~GlobalErrors()
{
	clear_errors();
}
