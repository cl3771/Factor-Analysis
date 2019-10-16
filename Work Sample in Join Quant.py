'''
    专门为Alpha-T策略进行因子分析的包
    主要提供与数据下载、因子分析相关的函数
    editor：Changqian Liu
    time:20190813
'''
#导入可能需要的库
import pandas as pd
import numpy as np
from numpy import NaN
from numpy import inf
import datetime
import time
import os
import re
from jqdatasdk import *
auth('13570680272','Lhllxlcq697196')

class Data_generation:
    
    #初始化
    def __init__(self, root, end_date, count):
        
        self.root = root
        self.end_date = end_date
        self.count = count
        
        self.main_data_generation_process(self.root,self.end_date,self.count)
        
               
               
    #创建路径的函数
    def mkdir(self,path):
        # 去除首位空格
        path=path.strip()
        # 去除尾部 / 符号
        path=path.rstrip("/")
        isExists=os.path.exists(path)
        # 判断结果
        if not isExists:
            # 如果不存在则创建目录
            # 创建目录操作函数
            os.makedirs(path) 

            print(path +' 创建成功')
            return True
        else:
            # 如果目录存在则不创建，并提示目录已存在
            print(path+' 目录已存在')
            return False  

    #下载股票分钟价格数据、日价格数据到本地
    def download_price_info(self,rootpath,second_root,stocklist,save_daylist):
        start_date = save_daylist[0]
        start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d')
        start_year = start_date.year
        start_month = start_date.month
        start_day = start_date.day
        end_date = save_daylist[-1]
        end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d')
        end_year = end_date.year
        end_month = end_date.month
        end_day = end_date.day
        fields=['open', 'close', 'low', 'high', 'volume', 'money', 'high_limit','low_limit']
        print("正在下载该时间段票池所有股票的分钟价格信息")
        price_info_minute = get_price(stocklist, start_date = datetime.datetime(start_year,start_month,start_day,9,30,0), 
                               end_date=datetime.datetime(end_year,end_month,end_day,15,0,0), frequency='minute', 
                               fields = fields, skip_paused=False, fq='pre')
        print("正在下载该时间段票池所有股票的日价格信息")
        price_info_day = get_price(stocklist, start_date = start_date,end_date = end_date, frequency='1d', 
                               fields = fields, skip_paused=False, fq='pre')    

        for stock in stocklist:
            self.mkdir(rootpath + '/' + second_root + '/' + stock)
            #把该股票这段时期的日价格信息提出来，存入一个表格
            print("正在归档%s的日价格信息"%(stock))
            day_table_entry = dict()
            for field in fields:
                day_table_entry[field] = price_info_day[field][stock]
            day_table = pd.DataFrame(day_table_entry)
            day_table.reset_index(inplace = True)
            day_table.rename(columns = {'index':'time'},inplace = True)
            #创造路径储存股票的日价格数据
            stockpath_day = stock + '_日价格信息表.xlsx'
            filename1 = rootpath + "/" + second_root + '/' + stock + '/'+ stockpath_day
            day_table.to_excel(filename1,encoding = 'utf-8',index = True)

            #把该股票这段时期的分钟价格信息提取出来，存入一个表格
            print("正在归档%s的分钟价格信息"%(stock))
            min_table_entry = dict()
            for field in fields:
                min_table_entry[field] = price_info_minute[field][stock]
            min_table = pd.DataFrame(min_table_entry)
            min_table.reset_index(inplace = True)
            min_table.rename(columns = {'index':'time'},inplace = True)
            #我们要稍微做点改动，增加振幅“amplitude”这个字段，计算分钟振幅
            datelist = list(day_table['time'].apply(lambda x: datetime.datetime.strftime(x,'%Y-%m-%d')).values)
            closelist = list(day_table['close'].values)
            openlist = list(day_table['open'].values)
            timelist1 = min_table['time'].reset_index(drop = True)
            high_list1 = list(min_table['high'].values)
            low_list1 = list(min_table['low'].values)
            #设置空白列表用来装每分钟振幅计算结果
            amplitude_list = list()
            for i in range(len(timelist1)):
                time = timelist1[i]
                date = datetime.datetime.strftime(time,'%Y-%m-%d')
                try:
                    date_index = datelist.index(date)
                    #如果不是第一天的分钟数据，我们的分钟振幅计算需要用到昨日的收盘价。
                    if date_index != 0:
                        yesterdayclose = closelist[date_index - 1]
                    #如果是第一天的分钟数据，我们的分钟振幅计算的分母取当天的开盘价，假装作为昨天的收盘价。
                    else:
                        yesterdayclose = openlist[date_index]
                except ValueError:
                    print('%s的信息在分钟价格数据出现但是在日价格数据中不见了'%time)
                    yesterdayclose = np.nanmean(closelist)

                high = high_list1[i]
                low = low_list1[i]

                amplitude_list.append(((high - low)/yesterdayclose))

            min_table['amplitude'] = amplitude_list

            #创造路径储存股票的分钟价格数据
            stockpath = stock + '_分钟价格信息表.xlsx'
            filename2 = rootpath + "/" + second_root + '/' + stock + '/'+ stockpath
            min_table.to_excel(filename2,encoding = 'utf-8',index = True)
        print('股票价格数据下载完毕')


    #下载财务数据存储到文件中
    def download_financeinfo(self,rootpath, third_path, save_daylist):
        finance_data = pd.DataFrame()
        first_date = save_daylist[0]
        last_date = save_daylist[-1]
        path = rootpath + "/" + third_path
        self.mkdir(path)
        for i in range(len(save_daylist)):
            date = save_daylist[i]
            print("开始处理%s的财务数据"%(date))
            daydf = get_fundamentals(query(valuation),date)
            finance_data = pd.concat([finance_data,daydf],axis = 0).reset_index(drop = True)
        finance_data.to_excel(path + "/" + "%s-%s.xlsx"%(first_date,last_date))

    #生成stocklist中每个股票每一天的因子值，存入表格
    def stockfactor_generator(self,rootpath, second_root, third_root, stocklist,save_daylist):
        print(' 开始生成该时间区间所有股票在该时间区间的所有因子值')
        #先读入老的股票分级表格，以备后面使用
        stockrankdf=pd.read_excel('/Users/changqianliu/Desktop/运行文件/运营数据/Alpha-T个股近1月收益排名_20190709.xlsx',encoding="gbk")
        #再读入财务数据，以便后面调取财务因子
        first_date = save_daylist[0]
        last_date = save_daylist[-1]
        finance_path = rootpath + "/" + third_root
        finance_data = pd.read_excel(finance_path + "/" + "%s-%s.xlsx"%(first_date,last_date))
        storage_factors= pd.DataFrame()
        for i in range(len(stocklist)):
            stock = stocklist[i]
            #找到该股票的交易价格记录
            stockpath1 = stock + '_分钟价格信息表.xlsx'
            stockpath2 = stock + '_日价格信息表.xlsx'
            filename1 = rootpath + "/" + second_root + '/' + stock + '/'+ stockpath1
            filename2 = rootpath + "/" + second_root + '/' + stock + '/'+ stockpath2
            price_min = pd.read_excel(filename1)
            price_day = pd.read_excel(filename2)
            #找到该股票的旧的评级
            rank = stockrankdf[stockrankdf["code"]==stock]
            if rank.empty:
                rank = 1
            else:
                rank = rank["ALPHA-T回测收益分级"].values[0]
            #建立list以备储存历史交易额数据
            trading_vol_history = list()
            #建立list以储存历史盘中涨跌停记录
            high_low_limit_middle = list()
            high_low_limit_close = list()
            for j in range(len(save_daylist)):
                day = save_daylist[j]
                if day == "2019-07-13":
                    continue
                print("正在处理股票%s在%s的因子"%(stock,day))
                stockinfo_entry = dict()
                stockinfo_entry['交易日期'] = day
                stockinfo_entry['股票代码'] = stock
                price_today_min = price_min[price_min['time'].apply(lambda x: datetime.datetime.strftime(x,'%Y-%m-%d'))==day].reset_index(drop = True)
                price_today_day = price_day[price_day['time'].apply(lambda x: datetime.datetime.strftime(x,'%Y-%m-%d'))==day].reset_index(drop = True)

                #调用日交易记录，输入10日盘中涨跌停次数
                if price_today_day['high'].values[0] == price_today_day['high_limit'].values[0] or price_today_day['low'].values[0] == price_today_day['low_limit'].values[0]:
                    high_low_limit_middle.append(1)
                else:
                    high_low_limit_middle.append(0)
                high_low_limit_middle = high_low_limit_middle[-10:]
                if len(high_low_limit_middle) >= 10:
                    stockinfo_entry['10日盘中涨跌停次数'] = np.sum(high_low_limit_middle)
                else:
                    stockinfo_entry['10日盘中涨跌停次数'] = NaN

                #顺便输入过去10日涨跌停且封盘次数
                if price_today_day['close'].values[0] == price_today_day['high_limit'].values[0] or price_today_day['close'].values[0] == price_today_day['low_limit'].values[0]:
                    high_low_limit_close.append(1)
                else:
                    high_low_limit_close.append(0)
                high_low_limit_close = high_low_limit_close[-10:]
                if len(high_low_limit_close) >= 10:
                    stockinfo_entry['10日盘中涨跌停且封盘次数'] = np.sum(high_low_limit_close)
                else:
                    stockinfo_entry['10日盘中涨跌停且封盘次数'] = NaN

                #调用日交易记录，输入交易额以及相关因子
                stockinfo_entry['当天交易额'] = price_today_day['money'].values[0]
                trading_vol_history.append(stockinfo_entry['当天交易额'])
                trading_vol_history = trading_vol_history[-20:]
                if len(trading_vol_history) >= 2:
                    stockinfo_entry['两日平均交易额'] = np.mean(trading_vol_history[-2:])
                else:
                    stockinfo_entry['两日平均交易额'] = NaN
                if len(trading_vol_history) >= 10:
                    stockinfo_entry['十日平均交易额（包含两日）'] = np.mean(trading_vol_history[-10:])
                else:
                    stockinfo_entry['十日平均交易额（包含两日）'] = NaN
                if not pd.isnull(stockinfo_entry['两日平均交易额']) and not pd.isnull(stockinfo_entry['十日平均交易额（包含两日）']):
                    stockinfo_entry['缩量指标%1'] = stockinfo_entry['两日平均交易额']/stockinfo_entry['十日平均交易额（包含两日）'] * 100
                else:
                    stockinfo_entry['缩量指标%1'] = NaN
                if not pd.isnull(stockinfo_entry['两日平均交易额']):
                    stockinfo_entry['交易额增长率%（环比前一日）'] = (1 - trading_vol_history[-2]/trading_vol_history[-1]) * 100
                else:
                    stockinfo_entry['交易额增长率%（环比前一日）'] = NaN
                if len(trading_vol_history) >= 12:
                    stockinfo_entry['十日平均交易额（不包含两日）'] = np.mean(trading_vol_history[-12:-2])
                else:
                    stockinfo_entry['十日平均交易额（不包含两日）'] = NaN
                if not pd.isnull(stockinfo_entry['两日平均交易额']) and not pd.isnull(stockinfo_entry['十日平均交易额（不包含两日）']):
                    stockinfo_entry['缩量指标%2'] = stockinfo_entry['两日平均交易额']/stockinfo_entry['十日平均交易额（不包含两日）'] * 100
                else:
                    stockinfo_entry['缩量指标%2'] = NaN

                #调用分钟交易记录，输入振幅数因子
                pricetoday_30min = price_today_min.iloc[:30]
                stockinfo_entry['开盘30min的分钟振幅大于千二的数目'] = len(pricetoday_30min[pricetoday_30min['amplitude']>0.002])
                stockinfo_entry['开盘30min的分钟振幅大于千三的数目'] = len(pricetoday_30min[pricetoday_30min['amplitude']>0.003])
                stockinfo_entry['开盘30min的分钟振幅大于千五的数目'] = len(pricetoday_30min[pricetoday_30min['amplitude']>0.005])
                pricetoday_sorted_min = price_today_min.sort_values(by = 'money', ascending = False)
                pricetoday_max30 = pricetoday_sorted_min.iloc[:30]
                stockinfo_entry['每日分钟成交额前30的振幅大于千二的数目'] = len(pricetoday_max30[pricetoday_max30['amplitude']>0.002])
                stockinfo_entry['每日分钟成交额前30的振幅大于千三的数目'] = len(pricetoday_max30[pricetoday_max30['amplitude']>0.003])
                stockinfo_entry['每日分钟成交额前30的振幅大于千五的数目'] = len(pricetoday_max30[pricetoday_max30['amplitude']>0.005])
                pricetoday_max10 = pricetoday_sorted_min.iloc[:10]
                stockinfo_entry['每日分钟成交额前10的振幅大于千二的数目'] = len(pricetoday_max10[pricetoday_max10['amplitude']>0.002])
                stockinfo_entry['每日分钟成交额前10的振幅大于千三的数目'] = len(pricetoday_max10[pricetoday_max10['amplitude']>0.003])
                stockinfo_entry['每日分钟成交额前10的振幅大于千五的数目'] = len(pricetoday_max10[pricetoday_max10['amplitude']>0.005])
                #下面从财务数据中添加市盈率、市净率、换手率、流通市值
                finance_stock_day = finance_data[(finance_data['code'] == stock) & (finance_data['day'] == day)]
                stockinfo_entry['流通市值'] = finance_stock_day['circulating_market_cap'].values[0]
                stockinfo_entry['市盈率'] = finance_stock_day['pe_ratio'].values[0]
                stockinfo_entry['换手率'] = finance_stock_day['turnover_ratio'].values[0]
                stockinfo_entry['市净率'] = finance_stock_day['pb_ratio'].values[0]
                #下面添加已有的股票级数
                stockinfo_entry['股票级数（旧）'] = rank

                stockinfo_entry = pd.DataFrame(stockinfo_entry,index = [0])

                storage_factors = pd.concat([storage_factors, stockinfo_entry],axis = 0).reset_index(drop = True)

        return storage_factors

    def return_dict_generator(self,df2):
        #为了获得第二日的收益率，创造字典
        accountdict = dict()
        accountlist = list(df2['资金账号'].unique())
        for account in accountlist:
            stockdict = dict()
            df3 = df2[df2['资金账号'] == account].reset_index(drop = True)
            stockcodelist = list(df3['股票代码'].unique())
            for stockcode in stockcodelist:
                return_dict = dict()
                df4 = df3[df3['股票代码'] == stockcode]
                stockdatelist = list(df4['交易日期'].unique())
                for day in stockdatelist:
                    df5 = df4[df4['交易日期']==day]
                    result_return = df5['当天收益率%'].iloc[0]
                    return_dict[day] = result_return
                stockdict[stockcode] = return_dict
            accountdict[account] = stockdict
        return accountdict

    def dataframe_update(self,accountdict,df2):
        second_day_return = list()
        nrow = len(df2)
        for i in range(nrow):
            print('处理在%s股票%s的数据'%(df2['交易日期'][i],df2['股票代码'][i]))
            account = df2['资金账号'][i]
            stockcode = df2['股票代码'][i]
            date = df2['交易日期'][i]
            date_returns = accountdict[account][stockcode]
            datelist = list(date_returns.keys())
            date_index = datelist.index(date)
            try:
                nextdate = datelist[date_index + 1]
                today = datetime.datetime.strptime(date,'%Y-%m-%d')
                tomorrow = datetime.datetime.strptime(nextdate,'%Y-%m-%d')
                diff_days = (tomorrow - today).days
                if diff_days == 1:
                    second_day_return.append(date_returns[nextdate])
                elif diff_days == 3 and today.weekday()+1 ==5:
                    second_day_return.append(date_returns[nextdate])
                else:
                    second_day_return.append(NaN)
            except IndexError:
                second_day_return.append(NaN)
        df2['第二日收益率%'] = second_day_return
        df2['第二日收益率%'] = df2['第二日收益率%'].apply(lambda x: float('%.4f' % x))
        return df2

    def main_data_generation_process(self,root,end_date,count):
        
        #读取某日的单票收益作为我们分析的数据来源
        df = pd.read_excel("/Users/changqianliu/Desktop/运行文件/运营数据/收益计算结果" + "/" + end_date + "/" + "汇总数据" + "/" + end_date + "_单票收益记录.xlsx",encoding = 'gbk')
        df=df.sort_values(by = ['交易日期','账户名','股票名称'],axis = 0,ascending = True).reset_index(drop = True)
        group = df.groupby('交易日期').size()
        #定义我们要追溯的日期数目
        daylist = list(group.iloc[-count:].index) #the daylist that we want to use for calculating the factors
        #为了补全信息，我们把前10天的数据也取了
        save_daylist = list(group.iloc[-(count+12):].index) #the daylist that we use to store the data needed.

        #定义几个变量，方便我们下载股票数据和存储
        first_date = save_daylist[0]
        last_date = save_daylist[-1]
        rootpath = root + '/' + '%s - %s'%(first_date,last_date)
        
        isExists=os.path.exists(rootpath)
        #如果该路径不存在，说明之前没有在根路径上下载过该时间段的因子分析数据，我们则需要自行下载。
        if not isExists:     
            second_root = '%s - %s'%(first_date,last_date) + '股票信息'
            third_root = '%s - %s'%(first_date,last_date) + '财务数据'

            #提取要处理的收益表格
            df1 = df[df['交易日期'].apply(lambda x: x in daylist)].reset_index(drop = True)
            stocklist = list(df1['股票代码'].unique())

            #根据stocklist以及saveday_list 下载股票分钟价格数据
            self.download_price_info(rootpath,second_root,stocklist,save_daylist)
            self.download_financeinfo(rootpath, third_root, save_daylist)

            #我们继续截取我们所关注时间段alpha-T的收益数据
            df2 = df1[['资金账号','交易日期','股票代码','股票级数','股票名称','当天交易额','当天收益',
                       '连续亏损天数','胜率%','连续盈利天数','撤单率%','十日亏损天数','十日盈利天数']].reset_index(drop = True)

            df2.rename(columns={'连续亏损天数': 'AT连续亏损天数', '胜率%': 'AT胜率%',
                               '连续盈利天数': 'AT连续盈利天数', '撤单率%': 'AT撤单率%','十日亏损天数':"AT十日亏损天数",
                              '十日盈利天数':'AT十日盈利天数'}, inplace=True)

            #计算算法在该股票的当天的收益率 = 当天收益/（当天交易额/2）
            df2['当天收益率%'] = df2['当天收益']/(df2['当天交易额']/2) * 100
            df2['当天收益率%'] = df2['当天收益率%'].apply(lambda x: float('%.4f' % x))
            df2 = df2.dropna().reset_index(drop = True)

            #我们建立另一个表格factor_info储存每个股票每一天的因子数据
            storage_factors = self.stockfactor_generator(rootpath, second_root, third_root, stocklist,save_daylist)
            storage_factors.to_excel(rootpath + '/' + 'factor_info.xlsx')
            #storage_factors = pd.read_excel(rootpath + '/' + 'factor_info.xlsx')

            #计算完收益率我们就可以把‘当天收益
            df2 = df2.drop(['当天收益'],axis = 1)

            #为了获得第二日的收益率，创造字典
            accountdict = self.return_dict_generator(df2)
            df2 = self.dataframe_update(accountdict,df2)
            df2 = df2.rename(columns={'第二日收益率%':'Target'})

            #我们舍弃target不存在以及其它因子为空的记录
            df3 = df2.dropna().reset_index(drop = True)

            #我们舍弃因子以及收益为空的记录
            df3 = df2.dropna().reset_index(drop = True)
            df3.to_excel(rootpath + "/" + "df3.xlsx")
        #如果该路径已经存在，说明之前就已经在根路径下载过了该时间段的因子数据了。
        else:
            print("已经生成该区间的数据，该数据存储在%s"%(rootpath))


#进行主要因子分析的模块儿
                  
class Factor_analysis:
    
    #初始化
    def __init__(self,df,factor_info,factor):
        
        self.df = df
        self.factor_info = factor_info
        self.factor = factor
        self.start_date = df['交易日期'].unique()[0]
        self.end_date = df['交易日期'].unique()[-1]
    
    #取小数的函数
    def get3float(self,float_num):
        return float('%.3f' % float_num)
    
     #Alpha-T基准策略。
    def baseline_generator(self):
        index = ["交易日期","当天等权收益率","累计收益率","胜率","盈亏比","筛除占比","剩余交易额占比","参与股票数目"]
        new_daylist = list(self.df['交易日期'].unique())
        baseline = pd.DataFrame()
        cumulative_profit = list()
        for i in range(len(new_daylist)):
            baseline_entry = dict()
            day = new_daylist[i]
            subdf = self.df[self.df['交易日期'] == day]
            #交易日期
            baseline_entry[index[0]] = day
            #当天等权收益率
            baseline_entry[index[1]] = subdf['当天收益率%'].sum()/len(subdf)
            cumulative_profit.append(baseline_entry[index[1]])
            #累积收益率
            baseline_entry[index[2]] = sum(cumulative_profit)
            #胜率
            returns_of_theday = subdf['当天收益率%'].values
            if_positive = np.array([int(i>0) for i in returns_of_theday])
            baseline_entry[index[3]] = np.sum(if_positive)/len(subdf)

            #盈亏金额比
            money_win = np.sum(np.multiply(if_positive,returns_of_theday))
            money_loss = money_win - np.sum(returns_of_theday)

            baseline_entry[index[4]]  = money_win/money_loss
            

            #筛除占比
            baseline_entry[index[5]] = 0

            #剩余交易额占比
            baseline_entry[index[6]] = 1

            #参与股票数目
            baseline_entry[index[7]] = len(subdf['股票代码'].unique())

            baseline_entry = pd.DataFrame(baseline_entry, index = [0])

            baseline = pd.concat([baseline,baseline_entry],axis = 0).reset_index(drop = True)

        return baseline
    
    #根据特定因子区间确定的策略
    def strategy_factor_interval(self,lower_bound,upper_bound,freq):
        df3 = self.df
        factor_info = self.factor_info
        factor = self.factor
        new_daylist = list(df3['交易日期'].unique())
        index = ["交易日期","当天等权收益率","累计收益率","胜率","盈亏比","筛除占比","剩余交易额占比","参与股票数目",'票池因子区间']
        strategy = pd.DataFrame()
        cumulative_profit = list()
        #先处理第一天该策略的收益 -- 和基准一毛一样
        firstday_entry = dict()
        firstday = new_daylist[0]
        subdf = df3[df3['交易日期'] == firstday]
        #交易日期
        firstday_entry[index[0]] = firstday
        #当天等权收益率
        firstday_entry[index[1]] = subdf['当天收益率%'].sum()/len(subdf)
        cumulative_profit.append(firstday_entry[index[1]])
        #累计收益率
        firstday_entry[index[2]] = sum(cumulative_profit)
        #胜率
        returns_of_firstday = subdf['当天收益率%'].values
        if_positive = np.array([int(i>0) for i in returns_of_firstday])
        firstday_entry[index[3]] = np.sum(if_positive)/len(subdf)
        #盈亏金额比
        money_win = np.sum(np.multiply(if_positive,returns_of_firstday))
        money_loss = money_win - np.sum(returns_of_firstday)
        firstday_entry[index[4]] = money_win/money_loss

        #筛除占比
        firstday_entry[index[5]] = 0

        #剩余交易金额占比
        firstday_entry[index[6]] = 1

        #当日参与股票数目
        firstday_entry[index[7]] = len(subdf['股票代码'].unique())

        #票池因子区间
        firstday_entry[index[8]] = NaN

        firstday_entry = pd.DataFrame(firstday_entry, index = [0])

        strategy = pd.concat([strategy,firstday_entry],axis = 0).reset_index(drop = True)

        #创建一个列表，其中是需要根据因子调整仓位的日期
        newdaylist = list(df3['交易日期'].unique())
        num_days = len(new_daylist)
        duration = freq #每个调整因子的日期间隔天数(一月是20天，半个月10天，一周是5天)
        num_period = num_days//duration
        if num_days % duration == 0:
            location = np.arange(0,num_period,1) * duration
        else:
            location = np.arange(0,num_period + 1,1) * duration
        days_for_adjustment = np.array(new_daylist)[location]
        for i in range(1,len(new_daylist)):
            yesterday = new_daylist[i-1]
            today = new_daylist[i]
            if yesterday in days_for_adjustment: #若前一天刚好是筛选票池的日子，那我们今天就应该更新股票池
                factor_yesterday = factor_info[factor_info['交易日期'] == yesterday]#导入昨天的股票信息，根据昨日信息创建票池
                all_stocks = factor_yesterday['股票代码'].values
                all_stocks_factors = factor_yesterday[factor].values
                order = np.argsort(all_stocks_factors, )
                all_stocks = all_stocks[order]
                all_stocks_factors = all_stocks_factors[order]

                stockpool = all_stocks[(all_stocks_factors<upper_bound) & (all_stocks_factors>=lower_bound)]  
                stockpool_factors = all_stocks_factors[(all_stocks_factors<upper_bound) & (all_stocks_factors>=lower_bound)]

            subdf_today = df3[df3['交易日期'] == today] #提取基准策略中今日参与交易股票信息
            targetdf_today = subdf_today[subdf_today['股票代码'].apply(lambda x: x in stockpool)]

            if targetdf_today.empty:
                everyday_entry = dict()
                everyday_entry[index[0]] = today
                everyday_entry[index[1]] = 0
                cumulative_profit.append(everyday_entry[index[1]])
                everyday_entry[index[2]] = sum(cumulative_profit)
                everyday_entry[index[3]] = NaN
                everyday_entry[index[4]] = NaN
                everyday_entry[index[5]] = NaN
                everyday_entry[index[6]] = NaN
                everyday_entry[index[7]] = NaN
                everyday_entry[index[8]] = NaN
                everyday_entry = pd.DataFrame(everyday_entry, index = [0])
                strategy = pd.concat([strategy,everyday_entry],axis = 0).reset_index(drop = True)
            else:

                everyday_entry = dict()
                today_returns = targetdf_today['当天收益率%'].values

                #交易日期
                everyday_entry[index[0]] = today

                #当天等权收益率
                everyday_entry[index[1]] = np.sum(today_returns)/len(today_returns)
                cumulative_profit.append(everyday_entry[index[1]])
                #累计收益率
                everyday_entry[index[2]] = sum(cumulative_profit)

                #当天胜率
                if_positive = np.array([int(i>0) for i in today_returns])
                everyday_entry[index[3]] = np.sum(if_positive)/len(today_returns)

                #盈亏金额比
                money_win = np.sum(np.multiply(if_positive,today_returns))
                money_loss = money_win - np.sum(today_returns)

                everyday_entry[index[4]] = money_win/money_loss

                #筛除占比
                stocks_stay = len(targetdf_today['股票代码'].unique())
                stocks_total = len(subdf_today['股票代码'].unique())
                percent_abandom = (stocks_total-stocks_stay)/stocks_total
                everyday_entry[index[5]] = percent_abandom

                #剩余交易额占比
                today_money = subdf_today['当天交易额'].sum()
                money_stay = targetdf_today['当天交易额'].sum()
                money_ratio = money_stay/today_money
                everyday_entry[index[6]] = money_ratio

                #当日参与策略股票数目
                everyday_entry[index[7]] = len(targetdf_today['股票代码'].unique())

                #票池因子区间
                everyday_entry[index[8]] = "[%s,%s]"%(stockpool_factors[0],stockpool_factors[-1])

                everyday_entry = pd.DataFrame(everyday_entry, index = [0])

                strategy = pd.concat([strategy,everyday_entry],axis = 0).reset_index(drop = True)

        return strategy
    
    #创建分位数策略，函数最终返回一个以时间为index、以各个层级策略的各个指标为columns的dataframe。
    def strategy_comparison(self,freq,fraction):
        df3 = self.df
        factor_info = self.factor_info
        factor = self.factor
        new_daylist = list(df3['交易日期'].unique())
        index = ["交易日期%s","当天等权收益率%s","累计收益率%s","胜率%s","盈亏比%s","筛除占比%s","剩余交易额占比%s",
                 "参与股票数目%s","票池因子区间%s"]
        all_strategies = pd.DataFrame()
        for n in range(1,fraction+1):
            strategy = pd.DataFrame()
            cumulative_profit = list()
            #先处理第一天该策略的收益 -- 和基准一毛一样
            firstday_entry = dict()
            firstday = new_daylist[0]
            subdf = df3[df3['交易日期'] == firstday]
            #交易日期
            firstday_entry[index[0]%(n)] = firstday
            #当天等权收益率
            firstday_entry[index[1]%(n)] = subdf['当天收益率%'].sum()/len(subdf)
            cumulative_profit.append(firstday_entry[index[1]%(n)])
            #累计收益率
            firstday_entry[index[2]%(n)] = sum(cumulative_profit)
            #胜率
            returns_of_firstday = subdf['当天收益率%'].values
            if_positive = np.array([int(i>0) for i in returns_of_firstday])
            firstday_entry[index[3]%(n)] = np.sum(if_positive)/len(subdf)
            #盈亏金额比
            money_win = np.sum(np.multiply(if_positive,returns_of_firstday))
            money_loss = money_win - np.sum(returns_of_firstday)
            firstday_entry[index[4]%(n)] = money_win/money_loss

            #筛除占比
            firstday_entry[index[5]%(n)] = 0

            #剩余交易金额占比
            firstday_entry[index[6]%(n)] = 1

            #当日参与股票数目
            firstday_entry[index[7]%(n)] = len(subdf['股票代码'].unique())

            #票池因子区间
            firstday_entry[index[8]%(n)] = NaN

            firstday_entry = pd.DataFrame(firstday_entry, index = [0])

            strategy = pd.concat([strategy,firstday_entry],axis = 0).reset_index(drop = True)

            #创建一个列表，其中是需要根据因子调整仓位的日期
            newdaylist = list(df3['交易日期'].unique())
            num_days = len(new_daylist)
            duration = freq #每个调整因子的日期间隔天数(一月是20天，半个月10天，一周是5天)
            num_period = num_days//duration
            if num_days % duration == 0:
                location = np.arange(0,num_period,1) * duration
            else:
                location = np.arange(0,num_period + 1,1) * duration
            days_for_adjustment = np.array(new_daylist)[location]
            for i in range(1,len(new_daylist)):
                yesterday = new_daylist[i-1]
                today = new_daylist[i]
                if yesterday in days_for_adjustment: #若前一天刚好是筛选票池的日子，那我们今天就应该更新股票池
                    factor_yesterday = factor_info[factor_info['交易日期'] == yesterday]#导入昨天的股票信息，根据昨日信息创建票池
                    all_stocks = factor_yesterday['股票代码'].values
                    all_stocks_factors = factor_yesterday[factor].values
                    order = np.argsort(all_stocks_factors, )
                    all_stocks = all_stocks[order]
                    all_stocks_factors = all_stocks_factors[order]
                    num = len(all_stocks)
                    qn = int(round(n/fraction * num))
                    qn_star = int(round((n-1)/fraction * num))
                    stockpool = all_stocks[qn_star:qn]
                    stockpool_factors = all_stocks_factors[qn_star:qn]

                subdf_today = df3[df3['交易日期'] == today] #提取基准策略中今日参与交易股票信息
                targetdf_today = subdf_today[subdf_today['股票代码'].apply(lambda x: x in stockpool)]
                if targetdf_today.empty:
                    everyday_entry = dict()
                    everyday_entry[index[0]%(n)] = today
                    everyday_entry[index[1]%(n)] = 0
                    cumulative_profit.append(everyday_entry[index[1]%(n)])
                    everyday_entry[index[2]%(n)] = sum(cumulative_profit)
                    everyday_entry[index[3]%(n)] = NaN
                    everyday_entry[index[4]%(n)] = NaN
                    everyday_entry[index[5]%(n)] = NaN
                    everyday_entry[index[6]%(n)] = NaN
                    everyday_entry[index[7]%(n)] = NaN
                    everyday_entry[index[8]%(n)] = NaN
                    everyday_entry = pd.DataFrame(everyday_entry, index = [0])
                    strategy = pd.concat([strategy,everyday_entry],axis = 0).reset_index(drop = True)
                else:

                    today_returns = targetdf_today['当天收益率%'].values

                    everyday_entry = dict()
                    #交易日期
                    everyday_entry[index[0]%(n)] = today

                    #当天等权收益率
                    everyday_entry[index[1]%(n)] = np.sum(today_returns)/len(today_returns)
                    cumulative_profit.append(everyday_entry[index[1]%(n)])

                    #累计收益率
                    everyday_entry[index[2]%(n)] = sum(cumulative_profit)

                    #当天胜率
                    if_positive = np.array([int(i>0) for i in today_returns])
                    everyday_entry[index[3]%(n)] = np.sum(if_positive)/len(today_returns)

                    #盈亏金额比
                    money_win = np.sum(np.multiply(if_positive,today_returns))
                    money_loss = money_win - np.sum(today_returns)
                    if money_loss == 0:
                        everyday_entry[index[4]%(n)] = NaN
                    else:
                        everyday_entry[index[4]%(n)] = money_win/money_loss

                    #筛除占比
                    stocks_stay = len(targetdf_today['股票代码'].unique())
                    stocks_total = len(subdf_today['股票代码'].unique())
                    percent_abandom = (stocks_total-stocks_stay)/stocks_total
                    everyday_entry[index[5]%(n)] = percent_abandom

                    #剩余交易额占比
                    today_money = subdf_today['当天交易额'].sum()
                    money_stay = targetdf_today['当天交易额'].sum()
                    money_ratio = money_stay/today_money
                    everyday_entry[index[6]%(n)] = money_ratio

                    #当日参与策略股票数目
                    everyday_entry[index[7]%(n)] = len(targetdf_today['股票代码'].unique())

                    #票池因子区间
                    everyday_entry[index[8]%(n)] = "[%s,%s]"%(stockpool_factors[0],stockpool_factors[-1])

                    everyday_entry = pd.DataFrame(everyday_entry, index = [0])

                    strategy = pd.concat([strategy,everyday_entry],axis = 0).reset_index(drop = True)

            all_strategies = pd.concat([all_strategies,strategy],axis = 1)

        return all_strategies
    
    #绘制分位数策略的累计收益曲线
    def plot_cumulative_returns_by_quantiles(self,freq,fraction):
        factor =self.factor

        all_strategies = self.strategy_comparison(freq,fraction)
        baseline = self.baseline_generator()

        color = ['green','skyblue','purple','black','pink','yellow','orange','grey','brown','blue']
        y0 = baseline['累计收益率'].values
        time = baseline['交易日期'].values
        time_start = time[0]
        time_end = time[-1]

        import matplotlib as mpl
        import matplotlib.pyplot as plt
        import mpld3
        mpld3.enable_notebook()
        plt.rcParams['figure.figsize'] = [9,6]
        from pylab import mpl
        mpl.rcParams['font.sans-serif'] = ['SimHei']

        plt.figure()
        plt.title('%s - %s 累计收益率(不同%s)'%(time_start,time_end,factor))
        plt.plot(time, y0, color='red', label='baseline')

        for i in range(1,fraction+1):
            y = all_strategies['累计收益率%s'%(i)].values
            print('第%s部分的因子区间为：%s'%(i,all_strategies['票池因子区间%s'%(i)].values))
            print('第%s部分的参与的股票数目为：%s'%(i,all_strategies['参与股票数目%s'%(i)].values))
            plt.plot(time, y, color=color[i-1], label='strategy with %s smallest %s'%(i,factor))
        plt.ylim((0, 6))
        plt.legend() # 显示图例
        plt.xlabel('time')
        plt.ylabel('累计收益率')

        plt.show()
    
    #绘制区间策略的累积收益曲线
    def plot_cumulative_returns_by_intervals(self,lower_bound_list,upper_bound_list,freq):
        factor = self.factor
        
        #想要查看的因子区间策略的数量
        num_strategies = len(upper_bound_list)
        #基准策略
        baseline = self.baseline_generator()

        color = ['green','skyblue','purple','black','pink','yellow','orange','grey','brown','blue','coral']
        y0 = baseline['累计收益率'].values
        time = baseline['交易日期'].values
        time_start = time[0]
        time_end = time[-1]

        import matplotlib as mpl
        import matplotlib.pyplot as plt
        import mpld3
        mpld3.enable_notebook()
        plt.rcParams['figure.figsize'] = [9,6]
        from pylab import mpl
        mpl.rcParams['font.sans-serif'] = ['SimHei'] 

        plt.figure()
        plt.title('%s - %s 累计收益率(不同%s)'%(time_start,time_end,factor))
        plt.plot(time, y0, color='red', label='baseline')

        for i in range(num_strategies):
            upper_bound = upper_bound_list[i]
            lower_bound = lower_bound_list[i]
            strategy = self.strategy_factor_interval(lower_bound,upper_bound,freq)
            y = strategy['累计收益率'].values
            print('第%s个策略的因子区间为：%s'%(i+1,strategy['票池因子区间'].values))
            print('第%s个策略的参与的股票数目为：%s'%(i+1,strategy['参与股票数目'].values))
            plt.plot(time, y, color=color[i], label='因子为%s、因子区间为[%s,%s)的策略'%(factor,lower_bound,upper_bound))

        plt.ylim((0, 6))
        plt.legend() # 显示图例
        plt.xlabel('time')
        plt.ylabel('累计收益率')

        plt.show()

    #绘制分位数策略的收益表格
    def plot_returns_table_quantiles(self,freq_list,fraction):
        returns_table = pd.DataFrame()
        for freq in freq_list:
            table_entry = dict()
            all_strategies = self.strategy_comparison(freq,fraction)
            baseline = self.baseline_generator()
            table_entry['Mean Period Wise Return Baseline(bps)'] = self.get3float(np.nanmean(baseline['当天等权收益率'].values) * 100)
            for i in range(1,fraction + 1):
                y = all_strategies['当天等权收益率%s'%(i)].values
                y_mean = np.nanmean(y)
                table_entry['Mean Period Wise Return %s Quantile(bps)'%(i)] = self.get3float(y_mean * 100)

            table_entry['Mean Period Wise Spread(bps)'] = self.get3float((table_entry['Mean Period Wise Return %s Quantile(bps)'%(fraction)] - table_entry['Mean Period Wise Return %s Quantile(bps)'%(1)]))

            table_entry = pd.DataFrame(table_entry,index = ['freq = %s'%(freq)]).T
            returns_table = pd.concat([returns_table, table_entry],axis = 1)

        return returns_table
    
class Stock_pool_generator:
    
    def __init__(self,date_stockpool,factors_add_stocks,factors_remove_stocks):#,interval_add_stocks,interval_remove_stocks):
        self.date_stockpool = date_stockpool
        self.factors_add_stocks = factors_add_stocks
        self.factors_remove_stocks = factors_remove_stocks
        #self.interval_add_stocks = interval_add_stocks
        #self.interval_remove_stocks = interval_remove_stocks
        
        #所有因子排排坐
        self.all_factors = sorted(self.factors_add_stocks + self.factors_remove_stocks)
        
        #从总因子列表中获取与有效振幅相关的因子
        factor_amplitude = list()
        import re
        for factor in self.all_factors:
            pattern = re.compile(r'^(\D+)(\d+)[a-zA-Z]*分钟振幅大于千(\d+)的数目$')
            match = pattern.match(factor)
            if not pd.isnull(match):
                factor_amplitude.append(factor)
        factor_amplitude = sorted(factor_amplitude)
        self.factor_amplitude = factor_amplitude
        
        #从总因子列表中获取财务因子子列表
        financial_list = ['市净率','市盈率','流通市值','换手率']
        self.factor_financial = [i for i in financial_list if i in self.all_factors]
        
        #获取所有股票（不包括已经退市的股票）的名称
        self.all_securities = list(get_all_securities(types = ['stock'],date = self.date_stockpool).index)

        
        self.all_stocks_with_factor = self.generate_all_factors()
        
    #生成票池所有股票的10日涨跌停次数、3日平均振幅、3日平均交易额、期末收盘价
    def other_factor_generator(self):
        date_stockpool = self.date_stockpool
        all_securities = self.all_securities
        fields = ['open', 'close','low', 'high', 'volume', 'money', 'high_limit','low_limit']
        price_info = get_price(all_securities,count = 10, end_date = date_stockpool, frequency = '1d', fields=fields,
                                   skip_paused=False, fq='pre')
        out_df = pd.DataFrame()
        for stock in all_securities:
            factor_entry = dict()
            table_entry = dict()
            for field in fields:
                table_entry[field] = price_info[field][stock]
            table = pd.DataFrame(table_entry)
            nrow = len(table)

            #计算10日涨跌停次数
            high_low_limit_count = 0
            for i in range(nrow):
                AstockAday = table.iloc[i,:]
                if AstockAday['high'] == AstockAday['high_limit'] or AstockAday['low'] == AstockAday['low_limit']:
                    high_low_limit_count += 1

            factor_entry['10日涨跌停次数'] = high_low_limit_count

            #计算3日平均振幅
            high_list = list(table['high'].values)
            low_list = list(table['low'].values)
            close_list = list(table['close'].values)
            money_list = list(table['money'].values)
            day_amplitude = list()
            for i in range(nrow-3,nrow):
                high = high_list[i]
                low = low_list[i]
                yesterday_close = close_list[i-1]
                day_amplitude.append((high - low)/yesterday_close)
            day_amplitude = np.array(day_amplitude)
            mean_amplitude = np.nanmean(day_amplitude)
            mean_money_threeday = np.nanmean(np.array(money_list[nrow-3:]))
            factor_entry['3日平均振幅'] = mean_amplitude
            factor_entry['3日平均交易额'] = mean_money_threeday 
            factor_entry['期末收盘价'] = close_list[-1]

            factor_record = pd.DataFrame(factor_entry,index = [stock])

            out_df = pd.concat([out_df,factor_record],axis = 0)

        return out_df
    
    #生成全行业所有股票的与有效振幅数目相关的因子
    def effective_amplitude_num(self):
        date_stockpool = self.date_stockpool
        all_securities = self.all_securities
        factor_amplitude = self.factor_amplitude
        fields=['open','low','high','close','money']
        date_formal = datetime.datetime.strptime(date_stockpool,'%Y-%m-%d')
        year = date_formal.year
        month = date_formal.month
        daynum = date_formal.day
        price_minute_info = get_price(all_securities, start_date=datetime.datetime(year,month,daynum,9,30,0),
                               end_date=datetime.datetime(year,month,daynum,15,0,0), frequency='minute',
                               fields=fields, skip_paused=False, fq='pre', count=None)
        close_price_2days = get_price(all_securities,count =2,end_date = date_stockpool,frequency = '1d',fields = ['close'],
                                          skip_paused = False, fq = 'pre')['close']

        out_df = pd.DataFrame()
        for stock in all_securities:
            reach_limit_count = 0
            table_entry = dict()
            for field in fields:
                table_entry[field] = price_minute_info[field][stock]
            table = pd.DataFrame(table_entry)
            #提取该股票前一日收盘价
            close_yesterday = close_price_2days[stock].values[0]
            #开始计算该股票昨日的所有分钟振幅，并统计有效振幅数
            high_list = list(table['high'].values)
            low_list = list(table['low'].values)
            amplitude_list = list()
            for i in range(len(high_list)):
                high = high_list[i]
                low = low_list[i]
                amplitude_list.append(((high - low)/close_yesterday))
            table['amplitude'] = amplitude_list

            stock_effective_amp_dict = dict()
            for factor in factor_amplitude:
                pattern = re.compile(r'^(\D+)(\d+)[a-zA-Z]*分钟振幅大于千(\d+)的数目$')
                match = pattern.match(factor)
                types = match.groups()[0]
                n = int(match.groups()[1])
                k = float(match.groups()[2])/1000
                if types == '开盘':
                    table_open_n = table.iloc[:n,]
                    num_effective_amp = len(table_open_n[table_open_n['amplitude']>k])
                    stock_effective_amp_dict[factor] = num_effective_amp
                elif types == '交易额前':
                    table_max_n = table.sort_values(by = ['money'],ascending = False).iloc[:n,]
                    num_effective_amp = len(table_max_n[table_max_n['amplitude']>k])
                    stock_effective_amp_dict[factor] = num_effective_amp

            stock_effective_amp_record = pd.DataFrame(stock_effective_amp_dict,index = [stock])
            out_df = pd.concat([out_df,stock_effective_amp_record],axis = 0)


        return out_df
    
    #生成全行业股票所有股票的常见的财务因子
    def financial_factor_generator(self):
        date_stockpool = self.date_stockpool
        all_securities = self.all_securities
        financial_data = get_fundamentals(query(valuation),date_stockpool)
        financial_table = financial_data[['code','pe_ratio','turnover_ratio','pb_ratio','circulating_market_cap']]
        financial_table.set_index(['code'],inplace =True)
        financial_table.rename(columns = {'pe_ratio':'市盈率','turnover_ratio':'换手率', 
                                          'pb_ratio':'市净率','circulating_market_cap':'流通市值'},inplace = True)
        return financial_table
    
    #删除停牌的股票
    def drop_paused_stocks(self,out_table):
        date_stockpool = self.date_stockpool
        all_securities = self.all_securities
        fields = ['open', 'close', 'high', 'low', 'volume', 'money','paused']
        table = get_price(all_securities,end_date=date_stockpool,count = 1,fields = fields,skip_paused=False, fq='pre')
        table = table['paused'].T
        table.columns = ['是否停牌']
        table_paused = table[table['是否停牌'] == 1]
        list_paused = list(table_paused.index)
        out_table['股票代码']=list(out_table.index)
        out_table = out_table[out_table['股票代码'].apply(lambda x: x not in list_paused)]
        out_table = out_table.drop(['股票代码'],axis = 1)

        return out_table
    
    #删除科创板股票
    def drop_STAR_stocks(self,out_table):
        import re
        out_table['股票代码'] = list(out_table.index)
        pattern = re.compile(r'688(\d{3}).XSHG$')
        out_table = out_table[out_table['股票代码'].apply(lambda x: pd.isnull(pattern.match(x)))]
        out_table = out_table.drop(['股票代码'],axis = 1)
        return out_table
    
    #上面五个函数都为了这个函数作准备的
    def generate_all_factors(self):
        
        all_factors = self.all_factors
        factor_amplitude = self.factor_amplitude
        factor_financial = self.factor_financial

        #开始搜集每只股票的因子数据，index为股票名称，列名为因子值。
        out_table = pd.DataFrame()
        if len(factor_amplitude) != 0:
            amplitude_table = self.effective_amplitude_num()
            out_table = pd.concat([out_table,amplitude_table],axis = 1)
        if len(factor_financial) != 0:
            financial_table = self.financial_factor_generator()[factor_financial]
            out_table = pd.concat([out_table,financial_table],axis = 1)

        price_limit_amp_table = self.other_factor_generator()
        out_table = pd.concat([out_table,price_limit_amp_table],axis = 1)
        if not '10日涨跌停次数' in all_factors:
            out_table = out_table.drop(['10日涨跌停次数'],axis = 1)
        out_table = self.drop_paused_stocks(out_table)
        out_table = self.drop_STAR_stocks(out_table)
        
        return out_table 
   