top_dir=..
cur_dir=.
include $(top_dir)/Makefile.config

.PHONY: all dirs

SRCS  = $(wildcard *.c)
OBJS  = $(patsubst %,$(obj_dir)/%,$(patsubst %.c,%.o,$(notdir $(SRCS))))
DEPS := $(OBJS:.o=.d)
PROG = optsmr

-include $(DEPS)

all: $(PROG)

$(PROG): dirs $(OBJS)
	$(CC) -o $(bin_dir)/$(PROG) $(OBJS) $(LDFLAGS)

$(obj_dir)/%.o:%.c
	$(CC) $(CFLAGS) -c -w $< -o $@

clean:
	@$(RM) $(bin_dir)/$(PROG)
	@$(RM) $(obj_dir)

