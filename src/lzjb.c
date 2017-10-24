#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>

#include "enums.h"
#include "lzjb.h"

size_t lzjb_compress(void *s_start, void *d_start, size_t s_len, size_t d_len, int n)
{
	uchar_t *src = s_start;
	uchar_t *dst = d_start;
	uchar_t *cpy, *copymap;
	int copymask = 1 << (NBBYL - 1);
	int mlen, offset;
	uint16_t *hp;
#ifdef DEBUG
	uint16_t lempel[LEMPEL_SIZE] = {};
#else
	uint16_t lempel[LEMPEL_SIZE];	/* uninitialized; see above */
#endif

	while (src < (uchar_t *)s_start + s_len) {
		if ((copymask <<= 1) == (1 << NBBYL)) {
			if (dst >= (uchar_t *)d_start + d_len - 1 - 2 * NBBYL) {
				if (d_len != s_len)
					return (s_len);
				mlen = s_len;
				for (src = s_start, dst = d_start; mlen; mlen--)
					*dst++ = *src++;
				return (s_len);
			}
			copymask = 1;
			copymap = dst;
			*dst++ = 0;
		}
		if (src > (uchar_t *)s_start + s_len - MATCH_MAX) {
			*dst++ = *src++;
			continue;
		}
		hp = &lempel[((src[0] + 13) ^ (src[1] - 13) ^ src[2]) &
		    (LEMPEL_SIZE - 1)];
		offset = (intptr_t)(src - *hp) & OFFSET_MASK;
		*hp = (uint16_t)(uintptr_t)src;
		cpy = src - offset;
		if (cpy >= (uchar_t *)s_start && cpy != src &&
		    src[0] == cpy[0] && src[1] == cpy[1] && src[2] == cpy[2]) {
			*copymap |= copymask;
			for (mlen = MATCH_MIN; mlen < MATCH_MAX; mlen++)
				if (src[mlen] != cpy[mlen])
					break;
			*dst++ = ((mlen - MATCH_MIN) << (NBBYL - MATCH_BITS)) |
			    (offset >> NBBYL);
			*dst++ = (uchar_t)offset;
			src += mlen;
		} else {
			*dst++ = *src++;
		}
	}
	return (dst - (uchar_t *)d_start);
}

/*ARGSUSED*/
int lzjb_decompress(void *s_start, void *d_start, size_t s_len, size_t d_len, int n)
{
	uchar_t *src = s_start;
	uchar_t *dst = d_start;
	uchar_t *d_end = (uchar_t *)d_start + d_len;
	uchar_t *cpy, copymap;
	int copymask = 1 << (NBBYL - 1);

	while (dst < d_end) {
		if ((copymask <<= 1) == (1 << NBBYL)) {
			copymask = 1;
			copymap = *src++;
		}
		if (copymap & copymask) {
			int mlen = (src[0] >> (NBBYL - MATCH_BITS)) + MATCH_MIN;
			int offset = ((src[0] << NBBYL) | src[1]) & OFFSET_MASK;
			src += 2;
			if ((cpy = dst - offset) < (uchar_t *)d_start)
				return (-1);
			while (--mlen >= 0 && dst < d_end)
				*dst++ = *cpy++;
		} else {
			*dst++ = *src++;
		}
	}
	return (0);
}

