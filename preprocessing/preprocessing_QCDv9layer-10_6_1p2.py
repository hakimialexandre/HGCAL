#!/usr/bin/env python
# coding: utf-8


import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import os
import uproot
from datetime import date
import optparse


workdir=os.getcwd()
algo_name=['cl3d']
algo_name_full=[]


def deltar(df):
    df['deta']=df['cl3d_eta']-df['genjet_eta']
    df['dphi']=np.abs(df['cl3d_phi']-df['genjet_phi'])
    sel=df['dphi']>np.pi
    df['dphi']-=sel*(2*np.pi)
    return(np.sqrt(df['dphi']*df['dphi']+df['deta']*df['deta']))
    
def matching(event):
    return event.cl3d_pt==event.cl3d_pt.max()


def openroot(path, files, PU):
    os.chdir(path)
    algo={}
    algo_layer={}
    branches_gen=['event',
                  b'genjet_n',
 b'genjet_energy',
 b'genjet_pt',
 b'genjet_eta',
 b'genjet_phi',]
    branches_cl3d=['event', 'cl3d_n',
 'cl3d_id',
 'cl3d_pt',
 'cl3d_energy',
 'cl3d_eta',
 'cl3d_phi',
 'cl3d_clusters_n',
 'cl3d_clusters_id',
 'cl3d_layer_pt',
 'cl3d_showerlength',
 'cl3d_coreshowerlength',
 'cl3d_firstlayer',
 'cl3d_maxlayer',
 'cl3d_seetot',
 'cl3d_seemax',
 'cl3d_spptot',
 'cl3d_sppmax',
 'cl3d_szz',
 'cl3d_srrtot',
 'cl3d_srrmax',
 'cl3d_srrmean',
 'cl3d_emaxe',
 'cl3d_hoe',
 'cl3d_meanz',
 'cl3d_layer10',
 'cl3d_layer50',
 'cl3d_layer90',
 'cl3d_ntc67',
 'cl3d_ntc90',
 'cl3d_bdteg',
 'cl3d_quality',
 'cl3d_ipt',
 'cl3d_ienergy']
    #branches_T23=branches_cl3d+['cl3d_bdteg','cl3d_quality']
    
    test=uproot.open(os.listdir(path)[0])
    names=test.keys()
    for i in range(len(names)):
        names[i]=names[i].decode("utf8")
        

    for i,filename in enumerate(files,1):
        if i==1:
            gen=uproot.open(filename)[names[0]+'/HGCalTriggerNtuple'].pandas.df(branches_gen,flatten=True)
            for i in range(len(names)):
                algo[i]=uproot.open(filename)[names[i]+'/HGCalTriggerNtuple'].pandas.df(branches_cl3d,flatten=True)
                algo_layer[i]=uproot.open(filename)[names[i]+'/HGCalTriggerNtuple'].pandas.df(['cl3d_layer_pt'],flatten=True)

        else:
            gen=pd.concat([gen,uproot.open(filename)[names[0]+'/HGCalTriggerNtuple'].pandas.df(branches_gen,flatten=True)])
            for i in range(len(names)):
                algo[i]=pd.concat([algo[i],uproot.open(filename)[names[i]+'/HGCalTriggerNtuple'].pandas.df(branches_cl3d,flatten=True)])
                algo_layer[i]=pd.concat([algo_layer[i],uproot.open(filename)[names[i]+'/HGCalTriggerNtuple'].pandas.df(['cl3d_layer_pt'],flatten=True)])




    list={}
    flattened={}
    for i in algo:
        list[i]=algo_layer[i]['cl3d_layer_pt'].tolist()
        flattened[i] = [val for sublist in list[i] for val in sublist]
        algo[i]['layer']=flattened[i]

            
    return(gen, algo)

def preprocessing(path, files, savedir,  thr, PU):
    gen,algo=openroot(path, files, PU)
    n_rec={}
    algo_clean={}
    #no info on reached EE or original
    

    #clean gen from particles that are not the originals or didn't reach endcap
    sel=(np.abs(gen['genjet_eta'])>=1.6) & (np.abs(gen['genjet_eta'])<=2.9)
    gen=gen[sel]
    
    #split df_gen_clean in two by eta sign

    gen.set_index('event', inplace=True)

    for i in algo:
        #split clusters in two by eta sign

        #set the indices
        algo[i].set_index('event', inplace=True)

        #merging
        print('merging')
        algo_merged=gen.join(algo[i], how='left', rsuffix='_algo')

        #calculate deltar
        algo_merged['deltar']=deltar(algo_merged)


        #keep the unreconstructed values (NaN)
        sel=pd.isna(algo_merged['deltar']) 
        unmatched=algo_merged[sel]

        unmatched['matches']=False


        #select deltar under thr
        sel=algo_merged['deltar']<=thr
        algo_merged=algo_merged[sel]


        #matching
        group=algo_merged.groupby('event')
        n_rec[i]=group['cl3d_pt'].size()
        algo_merged['best_match']=group.apply(matching).array

        #keep only matched clusters 
        sel=algo_merged['best_match']==True
        algo_merged=algo_merged[sel]


        #remerge with NaN values
        print('concat')
        algo_clean[i]=pd.concat([algo_merged, unmatched], sort=False).sort_values('event') 

        algo_clean[i]['matches']=algo_clean[i]['matches'].replace(np.nan, True)
        algo_clean[i].drop(columns=['best_match'], inplace=True)


            #save files to savedir
        os.chdir(savedir)   
        gen.to_csv('gen_clean.csv')
        for i in algo:
            algo_clean[i].to_csv('{}.csv'.format(algo_name[i]))

        
if __name__=='__main__':
    parser = optparse.OptionParser()
    parser.add_option("-f","--file",type="string", dest="param_path", help="select the path to the parameters file")
   
    (opt, args) = parser.parse_args()

    param_path=opt.param_path
    import importlib
    import sys
    sys.path.append(param_path)
    param=importlib.import_module('param')
    path=param.path
    files=param.files
    thr=param.thr
    savedir=param.savedir
    PU=False
    
    preprocessing(path, files, savedir, thr, PU)

