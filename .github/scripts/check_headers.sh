#!/bin/bash

EXPECTED_HEADER=$(cat <<-END
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
END
)

ERRORS=0

for file in $(find . -name '*.go'); do
    HEADER=$(head -n 16 "$file")
    
    if [[ "$HEADER" != "$EXPECTED_HEADER" ]]; then
        echo "ERROR: Header does not match in file $file"
        ERRORS=$((ERRORS+1))
    fi
done

if [[ "$ERRORS" -ne 0 ]]; then
    exit 1
fi

echo "All files have correct headers"
