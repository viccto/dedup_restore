

#ifndef LZJB_H_
#define LZJB_H_
/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright 2007 Sun Microsystems, Inc.  All rights reserved.
 * Use is subject to license terms.
 */



/*
 * We keep our own copy of this algorithm for 2 main reasons:
 * 	1. If we didn't, anyone modifying common/os/compress.c would
 *         directly break our on disk format
 * 	2. Our version of lzjb does not have a number of checks that the
 *         common/os version needs and uses
 * In particular, we are adding the "feature" that compress() can
 * take a destination buffer size and return -1 if the data will not
 * compress to d_len or less.
 */


#define	MATCH_BITS	6
#define	MATCH_MIN	3
#define	MATCH_MAX	((1 << MATCH_BITS) + (MATCH_MIN - 1))
#define	OFFSET_MASK	((1 << (16 - MATCH_BITS)) - 1)
#define	LEMPEL_SIZE	256
#define NBBYL 8
typedef unsigned char uchar_t;
//typedef unsigned long int uintptr_t;
//typedef long int intptr_t;

/*ARGSUSED*/
size_t lzjb_compress(void *s_start, void *d_start, size_t s_len, size_t d_len, int n);

int lzjb_decompress(void *s_start, void *d_start, size_t s_len, size_t d_len, int n);

#endif
