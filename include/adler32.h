/*
 * adler32.h
 *
 *  Created on: 2012-6-8
 *      Author: badboy
 */

#ifndef ADLER32_H_
#define ADLER32_H_

#define CHAR_OFFSET 0

/*
 *   a simple 32 bit checksum that can be upadted from either end
 *   (inspired by Mark Adler's Adler-32 checksum)
 */
unsigned int adler32_checksum(char *buf, int len);

/*
 * adler32_checksum(X0, ..., Xn), X0, Xn+1 ----> adler32_checksum(X1, ..., Xn+1)
 * where csum is adler32_checksum(X0, ..., Xn), c1 is X0, c2 is Xn+1
 */
unsigned int adler32_rolling_checksum(unsigned int csum, int len, char c1, char c2);



#endif /* ADLER32_H_ */
