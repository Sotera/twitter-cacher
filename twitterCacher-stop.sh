#!/bin/bash

pid=`ps aux | grep TwitterCacher2 | awk '{print $2}'`
kill -9 $pid
