#!/bin/bash

EXPECTED_HEADER=$(cat <<-END
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
