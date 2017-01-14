##!/usr/local/anaconda2/bin/python
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pylab
import sys


result_file = sys.argv[1]
data = np.loadtxt(result_file)

row = data.shape[0]
col = data.shape[1]

f=open(sys.argv[2],"r")
rank=map(int,f.readline().split(" "))
iteration=map(int,f.readline().split(" "))
lamb=map(float,f.readline().split(" "))
alpha=map(float,f.readline().split(" "))
f.close()

#iteration = [5,10,15,20]
#rank = [50,80,100,120,150,200]
# lambda = [0.001 0.005 0.007  0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.12 0.14 0.16]

lambda_l = np.empty(shape=[len(rank),len(iteration)],dtype=list)
mse_l = np.empty(shape=[len(rank),len(iteration)],dtype=list)
map_l = np.empty(shape=[len(rank),len(iteration)],dtype=list)
time_l = np.empty(shape=[len(rank),len(iteration)],dtype=list)

for j in range(len(rank)) : 
    for k in range(len( iteration)) :
        lambda_l[j][k]=[]
        mse_l[j][k]=[]
        map_l[j][k]=[]
        time_l[j][k]=[]

for i in range(row):
    for j in range(len(rank)) : 
        for k in range(len( iteration)) :
            if(data[i][0] == rank[j] and data[i][1] == iteration[k]):
                lambda_l[j][k].append(data[i][2])
                mse_l[j][k].append(data[i][4])
                map_l[j][k].append(data[i][5])
                time_l[j][k].append(data[i][6])

title = 'K=10_rank_iteration---lambda'
colors = ['red','c','m','black','blue','green']
linestyles = ['-',':','-.','--','-',':']

for i in range(len(rank)):
    fig,axs=plt.subplots(nrows=3, ncols=1,figsize=(9,9))
    ax = axs[0]
    for j in range(len(iteration)):
        ax.plot(lambda_l[i][j],mse_l[i][j],label= "$ite"+str(iteration[j])+"$",color=colors[j],linestyle=linestyles[j], linewidth=2)
        ax.legend(loc='lower right',fontsize=12)
        ax.set_title('MSE rank='+str(rank[i]))
    ax = axs[1]
    for j in range(len(iteration)):
        ax.plot(lambda_l[i][j],time_l[i][j],label= "$ite"+str(iteration[j])+"$",color=colors[j],linestyle=linestyles[j], linewidth=2)
        ax.set_title('TIME rank='+str(rank[i]))
    ax = axs[2]
    for j in range(len(iteration)):
        ax.plot(lambda_l[i][j],map_l[i][j],label= "$ite"+str(iteration[j])+"$",color=colors[j],linestyle=linestyles[j], linewidth=2)
        ax.set_title('MAP rank='+str(rank[i]))
    plt.tight_layout()
    fig.savefig("./"+"K=10_rank="+str(rank[i])+"_iteration---lambda.jpg",dpi=100)

for j in range(len(iteration)):
    fig,axs=plt.subplots(nrows=3, ncols=1,figsize=(9,9))
    ax = axs[0]
    for i in range(len(rank)):
        ax.plot(lambda_l[i][j],mse_l[i][j],label= "$rank"+str(rank[i])+"$",color=colors[i],linestyle=linestyles[i], linewidth=2)
        ax.legend(loc='lower right',fontsize=12)
        ax.set_title('MSE iteration='+str(iteration[j]))
    ax = axs[1]
    for i in range(len(rank)):
        ax.plot(lambda_l[i][j],time_l[i][j],label= "$rank"+str(rank[i])+"$",color=colors[i],linestyle=linestyles[i], linewidth=2)
        ax.set_title('TIME iteration='+str(iteration[j]))
    ax = axs[2]
    for i in range(len(rank)):
        ax.plot(lambda_l[i][j],map_l[i][j],label= "$rank"+str(rank[i])+"$",color=colors[i],linestyle=linestyles[i], linewidth=2)
        ax.set_title('MAP iteration='+str(iteration[j]))
    plt.tight_layout()
    fig.savefig("./"+"K=10_iteration="+str(iteration[j])+"_rank---lambda.jpg",dpi=100)

#ax1=plt.subplot(211)
##plt.title(title)
#ax2=plt.subplot(212)
#
#plt.sca(ax1)
#plt.plot(lambda_l1,mse_l1,label= "$MSE-iteration5$",color='red',linestyle='-', linewidth=2)
#plt.plot(lambda_l2,mse_l2,label= "$MSE-iteration10$",color='c' ,linestyle=':', linewidth=2)
#plt.plot(lambda_l3,mse_l3,label= "$MSE-iteration15$",color='m' ,linestyle='-.',linewidth=2)
#plt.legend(loc='upper left')
#
#plt.sca(ax2)
#plt.plot(lambda_l1,map_l1,label= "$MAP-iteration5$",color='red',linestyle='-', linewidth=2)
#plt.plot(lambda_l2,map_l2,label= "$MAP-iteration10$",color='c' ,linestyle=':', linewidth=2)
#plt.plot(lambda_l3,map_l3,label= "$MAP-iteration15$",color='m' ,linestyle='-.',linewidth=2)
#
#
#plt.xlabel('lambda')
#plt.ylabel('MAP')
##plt.xlim(0.01,0.2)
##plt.ylim(0.0,0.30)
#plt.legend(loc='upper right')
#
#plt.show()
#fig.savefig("./"+title+".jpg")
