# Current System with TV Arrivals
import SimFunctions
import SimRNG
import SimClasses
import scipy.stats as sp
import numpy as np
import pandas as pd

def mean_confidence_interval(data, confidence=0.95):
    a = 1.0*np.array(data)
    n = len(a)
    m, se = np.mean(a), sp.sem(a)
    h = se * sp.t._ppf((1+confidence)/2., n-1)
    return m, m-h, m+h

ZSimRNG = SimRNG.InitializeRNSeed()

Queue1 = SimClasses.FIFOQueue()
Queue2 = SimClasses.FIFOQueue()
AgentPool1 = SimClasses.Resource()
AgentPool2 = SimClasses.Resource()
QueueTime1 = SimClasses.DTStat()
QueueTime2 = SimClasses.DTStat()
Calendar = SimClasses.EventCalendar()

Arr_list_1 = [] #primary_input
LES_1 = []
state_1 = []
QL_1 = []
delay_1 = [] #Target

les_1 = 0

Arr_list_2 = [] #primary_input
LES_2 = []
state_2 = []
QL_2 = []
delay_2 = [] #Target

les_2 = 0


s1=3.0
s2=3.0

AgentPool1.SetUnits(s1)
AgentPool2.SetUnits(s2)

MeanST1 = 6.0
MeanST2 = 6.0

phase1=2
phase2=3

routing_policy = 6
routing_policy_l = 12
routing_policy_h = 24

RunLength = 480

AllStats = []

ArrivalRates = [24, 48, 72, 84, 84, 72, 48, 24]
MaxRate = 84


# Picewise-constant arrival rate function
def PW_ArrRate(t):
    hour = int(t/60)
    if hour <= 7:
        return ArrivalRates[hour]
    else:
        return ArrivalRates[-1]

# Thinning method used to geenerate NSPP with the piecewise-constant arrival rate
# maximum rate = 79.48 per hour or 79.48/60 per minute
def NSPP():
    PossibleArrival = SimClasses.Clock + SimRNG.Expon(1/(MaxRate/60), 1)
    while SimRNG.Uniform(0, 1, 1) >= PW_ArrRate(PossibleArrival)/(MaxRate):
        PossibleArrival = PossibleArrival + SimRNG.Expon(1/(MaxRate/60), 1)
    nspp = PossibleArrival - SimClasses.Clock
    return nspp

def Arrival():
    SimFunctions.Schedule(Calendar, "Arrival", NSPP())
    global les_1
    global les_2
    
    
    if SimRNG.Uniform(0, 1, 1) > 0.33:
        # Finanical customer
        Class1Customer = SimClasses.Entity()
        Arr_list_1.append(Class1Customer.CreateTime)
        LES_1.append(les_1)
        state_1.append(AgentPool1.CurrentNumBusy)
        QL_1.append(Queue1.NumQueue())
        
        
        if AgentPool1.CurrentNumBusy < s1:
            AgentPool1.Seize(1)
            SimFunctions.SchedulePlus(Calendar, "EndOfService1", SimRNG.Erlang(phase1, MeanST1, 2),Class1Customer)
            QueueTime1.Record(0.0)
            les_1 =0
            delay_1.append(0)
            
        else:
            Queue1.Add(Class1Customer)
    else:
        # Contact management customer
        Class2Customer = SimClasses.Entity()
        Arr_list_2.append(Class2Customer.CreateTime)
        LES_2.append(les_2)
        state_2.append(AgentPool2.CurrentNumBusy)
        QL_2.append(Queue2.NumQueue())
        
        if AgentPool2.CurrentNumBusy < s2:
                
            AgentPool2.Seize(1)
            SimFunctions.SchedulePlus(Calendar,"EndOfService2",SimRNG.Erlang(phase2, MeanST2, 2), Class2Customer)
            QueueTime2.Record(0.0)
            les_2 =0
            delay_2.append(0)
        else:
            Queue2.Add(Class2Customer)

def EndOfService1(): 
    global les_1
    
    
    if Queue1.NumQueue() > 0:
        NextCustomer = Queue1.Remove()
        delay_1.append(SimClasses.Clock - NextCustomer.CreateTime)
        les_1 = SimClasses.Clock - NextCustomer.CreateTime
        QueueTime1.Record(SimClasses.Clock - NextCustomer.CreateTime)
        SimFunctions.SchedulePlus(Calendar, "EndOfService1", SimRNG.Erlang(phase1, MeanST1, 2), NextCustomer)
    else:
        AgentPool1.Free(1)

def EndOfService2(): 
    global les_2

    if Queue1.NumQueue() > routing_policy_l and Queue1.NumQueue() < routing_policy_h: #Routing Policy
        NextCustomer = Queue1.Remove()
        delay_1.append(SimClasses.Clock - NextCustomer.CreateTime)
        QueueTime1.Record(SimClasses.Clock - NextCustomer.CreateTime)
        SimFunctions.SchedulePlus(Calendar, "EndOfService2", SimRNG.Erlang(phase2, MeanST2, 2), NextCustomer)        
            
    
    elif Queue2.NumQueue() > 0:
        NextCustomer = Queue2.Remove()
        delay_2.append(SimClasses.Clock - NextCustomer.CreateTime)
        les_2 = SimClasses.Clock - NextCustomer.CreateTime
        QueueTime2.Record(SimClasses.Clock - NextCustomer.CreateTime)
        SimFunctions.SchedulePlus(Calendar, "EndOfService2", SimRNG.Erlang(phase2, MeanST2, 2), NextCustomer)
    else:
        AgentPool2.Free(1)

nst_df = pd.DataFrame.from_dict({'arrival_time':[], 
                                    'ql':[],
                                    'les':[],
 #                                   'lcs':[],
                                    'numBusy':[],
                                    'delay':[],
                                    'queue_type':[]})
for reps in range(0, 20, 1):

    SimFunctions.SimFunctionsInit(Calendar)#, TheQueues, TheCTStats, TheDTStats, TheResources])
    SimFunctions.Schedule(Calendar, "Arrival", NSPP())
    SimFunctions.Schedule(Calendar, "EndSimulation", RunLength)

    NextEvent = Calendar.Remove()
    SimClasses.Clock = NextEvent.EventTime

    if NextEvent.EventType == "Arrival":
        Arrival()

    while NextEvent.EventType != "EndSimulation":
        NextEvent = Calendar.Remove()
        SimClasses.Clock = NextEvent.EventTime

        if NextEvent.EventType == "Arrival":
            Arrival()
        elif NextEvent.EventType == "EndOfService1":
            EndOfService1()
        elif NextEvent.EventType == "EndOfService2":
            EndOfService2()

    delete = 100
    q1_data = {'arrival_time':Arr_list_1[delete:len(delay_1)], 
                'ql':QL_1[delete:len(delay_1)],
                'les':LES_1[delete:len(delay_1)],
                'numBusy':state_1[delete:len(delay_1)],
                'delay':delay_1[delete:]}
    q1_df = pd.DataFrame.from_dict(q1_data)
    q1_df['queue_type'] = 1
    q1_df['ql_est'] = q1_df['ql']*(MeanST1/s1)
    
    q1_df['ql_2_avg'] = q1_df['ql'].rolling(window=2).mean().fillna(0)
    q1_df['ql_5_avg'] = q1_df['ql'].rolling(window=5).mean().fillna(0)
    q1_df['ql_10_avg'] = q1_df['ql'].rolling(window=10).mean().fillna(0)
    q1_df['ql_20_avg'] = q1_df['ql'].rolling(window=20).mean().fillna(0)
    
    q1_df['dh_2_avg'] = q1_df['delay'].rolling(window=2).mean().fillna(0)
    q1_df['dh_5_avg'] = q1_df['delay'].rolling(window=5).mean().fillna(0)
    q1_df['dh_10_avg'] = q1_df['delay'].rolling(window=10).mean().fillna(0)
    q1_df['dh_20_avg'] = q1_df['delay'].rolling(window=20).mean().fillna(0)
    
    q1_df['arr_pct'] = q1_df['arrival_time']/RunLength
    
    q1_df['lcs'] = q1_df['les'].shift(1,fill_value=0)

    q2_data = {'arrival_time':Arr_list_2[delete:len(delay_2)], 
                'ql':QL_2[delete:len(delay_2)],
                'les':LES_2[delete:len(delay_2)],
                'numBusy':state_2[delete:len(delay_2)],
                'delay':delay_2[delete:]}
    q2_df = pd.DataFrame.from_dict(q2_data)
    q2_df['queue_type'] = 2
    q2_df['ql_est'] = q2_df['ql']*(MeanST2/s2)
    
    
    q2_df['ql_2_avg'] = q2_df['ql'].rolling(window=2).mean().fillna(0)
    q2_df['ql_5_avg'] = q2_df['ql'].rolling(window=5).mean().fillna(0)
    q2_df['ql_10_avg'] = q2_df['ql'].rolling(window=10).mean().fillna(0)
    q2_df['ql_20_avg'] = q2_df['ql'].rolling(window=20).mean().fillna(0)
    
    q2_df['dh_2_avg'] = q2_df['delay'].rolling(window=2).mean().fillna(0)
    q2_df['dh_5_avg'] = q2_df['delay'].rolling(window=5).mean().fillna(0)
    q2_df['dh_10_avg'] = q2_df['delay'].rolling(window=10).mean().fillna(0)
    q2_df['dh_20_avg'] = q2_df['delay'].rolling(window=20).mean().fillna(0)
    
    q2_df['arr_pct'] = q2_df['arrival_time']/RunLength
    
    q2_df['lcs'] = q2_df['les'].shift(1,fill_value=0)
    
    nst_df = pd.concat([nst_df,q2_df,q1_df])
    
    AllStats.append([QueueTime1.Mean(), QueueTime2.Mean(), Queue1.Mean(), Queue2.Mean(), AgentPool1.Mean(), AgentPool2.Mean()])
    
nst_df['NI'] = np.mean(nst_df['delay'])


nst_df.to_csv('Nstate_SimData.csv',index=False)
   
Results = pd.DataFrame(AllStats, columns=["Average wait in queue 1", "Average wait in queue 2", "Average number in queue 1", "Average number in queue 2", "Average number of busy AgentPools 1", "Average number of busy AgentPools 2"])
print ("Wait 1 CI", mean_confidence_interval(Results.loc[:,"Average wait in queue 1"]))
print ("Wait 2 CI", mean_confidence_interval(Results.loc[:,"Average wait in queue 2"]))
print ("Queue 1 CI", mean_confidence_interval(Results.loc[:,"Average number in queue 1"]))
print ("Queue 2 CI", mean_confidence_interval(Results.loc[:,"Average number in queue 2"]))

