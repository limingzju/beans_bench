
#ifdef __cplusplus
extern "C" {
#endif
/*
 *  Beansdb - A high available distributed key-value storage system:
 *
 *      http://beansdb.googlecode.com
 *
 *  Copyright 2010 Douban Inc.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Davies Liu <davies.liu@gmail.com>
 *
 */

#ifndef __FNV1A_H__
#define __FNV1A_H__

typedef unsigned int uint32_t;
uint32_t fnv1a(const char *key, int key_len);

#endif

#ifdef __cplusplus
}
#endif
