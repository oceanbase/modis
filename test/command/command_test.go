/*-
 * #%L
 * Modis
 * %%
 * Copyright (C) 2024 OceanBase
 * %%
 * Modis is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package command

func matchPrefixCase() map[string]string {
	cs := map[string]string{
		"abc?[a-z]":  "abc",
		"?abc":       "",
		"\\*[^abc]?": "*",
	}
	return cs
}

type patternMap map[string]bool

func matchCase(nocase bool) map[string]*patternMap {
	var cs map[string]*patternMap
	if !nocase {
		cs = map[string]*patternMap{
			"*": &patternMap{
				"":     true,
				"abcd": true,
				"*[*]": true,
			},
			"******a": &patternMap{
				"a":     true,
				"***a":  true,
				"bcdea": true,
				"abcd":  false,
			},
			"\\*?aaa": &patternMap{
				"*caaa": true,
				"abc":   false,
			},
			"[a-z][^0-9][z-a]?[a-z": &patternMap{
				"abz.a": true,
				"a1z.*": false,
				"abz.e": true,
			},
			"[a-z]*cat*[h][^b]*eyes*": &patternMap{
				"my cat has very bright eyes": true,
				"my dog has very bright eyes": false,
			},
			"h?llo": &patternMap{
				"hello": true,
				"healo": false,
			},
			"h??lo": &patternMap{
				"hello": true,
			},
			"h*o": &patternMap{
				"hello": true,
				"ho":    true,
			},
		}

	} else {
		cs = map[string]*patternMap{
			"[A-Z][0-9]*": &patternMap{
				"B1":    true,
				"B2000": true,
				"b2000": false,
			},
			"*A": &patternMap{
				"abcdA": true,
				"abcda": false,
				"Ae":    false,
			},
			"?A*C": &patternMap{
				"1AbcdC":   true,
				"cA12344C": true,
				"1abcdc":   false,
			},
		}
	}

	return cs
}
