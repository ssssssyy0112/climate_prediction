# this file is about data range visualization.
# It uses pandas with pyplot to draw bar or plot about 
# the mean value of several countries/regions.
# Region_code_map is based on the standard 2-code region code from Wikipedia.

import pandas as pd
import matplotlib.pyplot as plt

region_code_map = {"RU":"Russian","IL":"Israel","NO":"Norway","IT":"Italy","DK":"Denmark",
                   "AL":"Albania","RO":"Romania","GB":"United Kindom","AM":"Armenia","ES":"Spain",
                   "EE":"Estonia","CH":"Swiss","AZ":"Azerbaijan","TM":"Turkmenistan","SK":"Slovakia",
                   "GL":"Greeland","UA":"Ukraine","DZ":"Algeria","KZ":"Kazakhstan","DE":"Germany",
                   "NL":"Netherlands","IE":"Ireland","PL":"Poland","FR":"France","FI":"Finland",
                   "HU":"Hungary","LV":"Latvia","LT":"Lithuania","SE":"Sweden","HR":"Croatia",
                   "UZ":"Uzbekistan","RS":"Serbia","KG":"Kyrgyzstan","TJ":"Tajikistan","SI":"Slovenia",
                   "IS":"Iceland","AT":"Austria","SJ":"Svalbard/Jan Mayen"}
region_code_str = "\n".join([f'{k}:{v}' for k,v in region_code_map.items()])
year=["1990","01","01"]
year_str = "".join(year)
year_title = "_".join(year)
df = pd.read_csv("2/aggr_"+year_str+".csv")

plt.figure(figsize=(15,6))
plt.title(year_title+" High Temp",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("temprature(℃)",fontsize=12)
bars = plt.bar(df['region_code'],df['avg_high']/10,alpha=0.7, label='average high')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')
for bar in bars:
    yval = bar.get_height()
    y_height = yval
    if y_height<0:
        y_height-=1
    plt.text(bar.get_x() - bar.get_width()/2.0, y_height, round(yval, 1), va='bottom')
plt.legend()
plt.savefig("pictures/aggr_high_"+year_str+".png")
plt.clf()


plt.figure(figsize=(15,6))
plt.title(year_title+" Mean Temp",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("temprature(℃)",fontsize=12)
bars = plt.bar(df['region_code'],df['avg_mean']/10,alpha=0.7, label='average mean')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')
for bar in bars:
    yval = bar.get_height()
    y_height = yval
    if y_height<0:
        y_height-=1
    plt.text(bar.get_x() - bar.get_width()/2.0, y_height, round(yval, 1), va='bottom')
plt.legend()
plt.savefig("pictures/aggr_mean_"+year_str+".png")
plt.clf()


plt.figure(figsize=(15,6))
plt.title(year_title+" Low Temp",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("temprature(℃)",fontsize=12)
bars = plt.bar(df['region_code'],df['avg_low']/10,alpha=0.7, label='average low')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')
for bar in bars:
    yval = bar.get_height()
    y_height = yval
    if y_height<0:
        y_height-=1
    plt.text(bar.get_x() - bar.get_width()/2.0, y_height, round(yval, 1), va='bottom')
plt.legend()
plt.savefig("pictures/aggr_low_"+year_str+".png")
plt.clf()


plt.figure(figsize=(15,6))
plt.title(year_title+" Precipitation",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("precipitation(mm)",fontsize=12)
df1 = df.copy()
df1 = df1.dropna(subset=['avg_rain'])
bars = plt.bar(df1['region_code'],df1['avg_rain']/10,alpha=0.7, label='average precipitation')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')
for bar in bars:
    yval = bar.get_height()
    y_height = yval
    if y_height<0:
        y_height-=1
    plt.text(bar.get_x(), y_height, round(yval, 1), va='bottom')

plt.legend()
plt.savefig("pictures/aggr_rain_"+year_str+".png")
plt.clf()


plt.figure(figsize=(15,6))
plt.title(year_title+" Sunshine Duration",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("duration(h)",fontsize=12)
df1 = df.copy()
df1 = df1.dropna(subset=['avg_sunshine'])
bars = plt.bar(df1['region_code'],df1['avg_sunshine']/10,alpha=0.7, label='average sunshine hour')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')
for bar in bars:
    yval = bar.get_height()
    y_height = yval
    if y_height<0:
        y_height-=1
    plt.text(bar.get_x(), y_height, round(yval,1), va='bottom',fontsize=14)
plt.legend()
plt.savefig("pictures/aggr_sunshine_"+year_str+".png")
plt.clf()


plt.figure(figsize=(15,6))
plt.title(year_title+" Wind Speed",fontsize=14)
plt.xlabel("region code",fontsize=12)
plt.ylabel("wind speed(m/s)",fontsize=12)
df1 = df.copy()
df1 = df1.dropna(subset=['avg_speed'])
x = list(df1['region_code'])
y = list(df1['avg_speed'])
y = [i/10 for i in y]
bars = plt.plot(x,y,color='r',marker='o',label='average wind speed')
plt.text(0.99,0.85,region_code_str,fontsize=8,bbox=dict(boxstyle='round', facecolor='white', edgecolor='gray', alpha=0.7),
         transform=plt.gcf().transFigure, horizontalalignment='right', verticalalignment='top')

for i in range(len(x)):
    plt.text(x[i],y[i]+0.1,str(round(y[i],1)))

plt.legend()
plt.savefig("pictures/aggr_speed_"+year_str+".png")
plt.clf()