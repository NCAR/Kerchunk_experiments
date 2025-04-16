#!/usr/bin/env python
import os
import sys
sys.path.append('../src')
import separate_kerchunk



def test_separate():
    separate_kerchunk.separate('/glade/campaign/collections/rda/work/rpconroy/ARCO/d559000/wy1988.3d.json.json')

test_separate()
