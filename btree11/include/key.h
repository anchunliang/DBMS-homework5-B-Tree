#ifndef KEY_H
#define KEY_H

#include "bt.h"

int keyCompare(const void *key1, const void *key2, AttrType t);
int get_key_length(const void *key, const AttrType key_type);
int get_key_data_length(const void *key, const AttrType key_type, const NodeType ndtype);

static void fill_entry_key(Keytype *target, const void *key,
		AttrType key_type, int *pentry_key_len);
static void fill_entry_data(char *target, Datatype source, NodeType ndtype,
		int *pentry_data_len);

void make_entry(KeyDataEntry *target, AttrType key_type, const void *key,
		NodeType ndtype, Datatype data, int *pentry_len);
void get_key_data(void *targetkey, Datatype *targetdata,
		KeyDataEntry *psource, int entry_len, NodeType ndtype);

#endif
