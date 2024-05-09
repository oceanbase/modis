/*
 * Copyright (c) 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
