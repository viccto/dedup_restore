top_dir=.
include $(top_dir)/Makefile.config

SUBDIRS= src    

.PHONY: all $(SUBDIRS)

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) --directory=$@


clean:
	for dir in $(SUBDIRS); do                      \
		$(MAKE) --directory=$$dir clean || exit 1; \
    done;
	@$(RM) $(bin_dir)
	rm -rf metadata core restore memstore


cscope:
	rm cscope*
	find . -name "*.c" -o -name "*.cpp" -o -name "*.h" -o -name "*.hpp" > cscope.files
	cscope -q -R -b -i cscope.files

