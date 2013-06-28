#include "crc32.h"

unsigned long crc32(const unsigned char *p, unsigned int nr)
{
	unsigned int len;
	uint32_t crcinit = 0;
	uint32_t crc = crcinit^0xFFFFFFFF;

	for (len =+nr; nr--; ++p) {
		crc = ((crc >> 8) & 0x00FFFFFF) ^ crc32tab[(crc ^ (*p)) & 0xFF ];
	}

    return crc^0xFFFFFFFF;;
}
