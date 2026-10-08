#!/bin/sh
set -eu
umask 077
exec telegram-bot-api "$@"
