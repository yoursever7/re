import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import sys
import random
import linecache
import re

result_file = sys.argv[1] 
title = re.search('\d+\S*log',result_file).group(0).strip('_log')
f = open(result_file,"r")

len_n = len(f.readlines())
#num = int(sys.argv[2])
num = 9 
#print "lenth: %d" %len_n
lines = random.sample(range(1,len_n), num)
#print lines
row=3
col=3
fig,axs = plt.subplots(nrows=row, ncols=col,figsize=(10,10),dpi=100)
#fig.text(0.5,1,title,ha='center',va='top',fontsize=14)


for i in range(len(lines)):
    list4usr = linecache.getline(result_file,lines[i])
    s2 = list4usr.strip("(])\n").replace('], [','\t').replace(', ([','\t').split('\t')
    usr=s2[0]
    rec_l = map( int, s2[1].split(','))
    act_l = map( int, s2[2].split(','))
    #print i,lines[i],rec_l,act_l
    index_rec = []
    index_act = []
    
    for j in range(len(rec_l)):
        if (rec_l[j] in act_l):
            index_rec.append(j+1)
            index_act.append(act_l.index(rec_l[j] )+1)

    axs[i/row,i%col].plot(np.arange(11)*1,'--r', marker='*', markersize=6)
    axs[i/row,i%col].plot(index_act,index_rec,'o')
    axs[i/row,i%col].set_xticks(np.arange(0,11,1))
    axs[i/row,i%col].set_yticks(np.arange(0,11,1))
    axs[i/row,i%col].set_xlabel('actual list - user'+usr, fontsize=10)
    axs[i/row,i%col].set_ylabel('recomendation list', fontsize=10)
    axs[i/row,i%col].grid(True) 

plt.tight_layout()
fig.suptitle(title,fontsize=18)
fig.subplots_adjust(top=0.93)
#fig.subplots_adjust(top=0.91,hspace=0.4,wspace=0.55,bottom=0.09,right=0.9,left=0.1)
#plt.show()
fig.savefig("./rec_list.jpg",dpi=100)
