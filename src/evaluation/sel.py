from evaluate_config import *
import numpy as np
import sys
import os 

f = open(ALSMODEL_CONFIG_FILE,"w+")
data = np.loadtxt(RESULT_FILE)
print data.shape
print data.ndim
if data.ndim != 1:
    map = data[:,6]
    #print map
    max_index = np.argmax(map)
    #print max_index
    print >> f ,"%d %d %f %f"%(data[max_index][0],data[max_index][1],data[max_index][2],data[max_index][3])
else:
    print >> f ,"%d %d %f %f"%(data[0],data[1],data[2],data[3])
f.close()
