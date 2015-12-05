#!/bin/sh

kill `pgrep "server.*\.py"` 2>/dev/null
kill `pgrep "client.*\.py"` 2>/dev/null
kill `pgrep "Master.*\.py"` 2>/dev/null
