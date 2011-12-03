#
# Makefile for CS564 Minibase project.  Needs GNU make.
#
# Define DEBUGREL for some kind of debugging output (not from us, from
# the original Minibase implementors.)
#
# Warning: make depend overwrites this file.

.PHONY: depend clean backup setup

MAIN = btree

MINIBASE = ..

CC = g++

#CFLAGS = -DUNIX -Wall -g
CFLAGS = -g

INCLUDES = -I${MINIBASE}/include -I.

LFLAGS = -L. -lbtree -lm

SRCS = main.C btree_driver.C btfile.C btindex_page.C btleaf_page.C btree_file_scan.C key.C db.C new_error.C sorted_page.C system_defs.C

OBJS = $(SRCS:.C=.o)

$(MAIN):  $(OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) $(OBJS) -o $(MAIN) $(LFLAGS)

.C.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $<

depend: $(SRCS)
	makedepend $(INCLUDES) $^

clean:
	rm -f *.o *~ $(MAIN)
	rm -f my_output

backup:
	-mkdir bak
	cp Makefile *.[Ch] bak

run:
	rm -rf my_output
	./btree > my_output

# Grab the sources for a user who has only the makefile
setup:
	/bin/cp -i $(MINIBASE)/src/*.[Ch] .
	/bin/cp -i $(MINIBASE)/src/*.sample .

# DO NOT DELETE THIS LINE -- make depend needs it 
