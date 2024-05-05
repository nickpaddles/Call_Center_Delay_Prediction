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

Queue = SimClasses.FIFOQueue()
AgentPool = SimClasses.Resource()
QueueTime = SimClasses.DTStat()
Calendar = SimClasses.EventCalendar()

#For ML

Arr_list= [] #primary_input
LES = []
state = []
QL = []
delay = [] #Target
Service_L =  []


agent_pool_size = 6
AgentPool.SetUnits(agent_pool_size)

Mean_st = 6.0

phase=2

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
    global les
    SimFunctions.Schedule(Calendar, "Arrival", NSPP()) #schedule next arrival
    Customer = SimClasses.Entity()
    
    Arr_list.append(Customer.CreateTime)
    LES.append(les)
    state.append(AgentPool.CurrentNumBusy)
    QL.append(Queue.NumQueue())
    
    if AgentPool.CurrentNumBusy < agent_pool_size:
        AgentPool.Seize(1)
        service_t = SimRNG.Erlang(phase, Mean_st, 2)
        Service_L.append(service_t)
        SimFunctions.SchedulePlus(Calendar, "EndOfService", service_t,Customer)

        QueueTime.Record(0.0)

        les = 0
        delay.append(0)
    else:
        Queue.Add(Customer)
    

def EndOfService(): 
    global les
    if Queue.NumQueue() > 0:
        NextCustomer = Queue.Remove()
        QueueTime.Record(SimClasses.Clock - NextCustomer.CreateTime)
        SimFunctions.SchedulePlus(Calendar, "EndOfService", SimRNG.Erlang(phase, Mean_st, 2), NextCustomer)
        
        delay.append(SimClasses.Clock - NextCustomer.CreateTime)
        les = SimClasses.Clock - NextCustomer.CreateTime

    else:
        AgentPool.Free(1)


mms_df = pd.DataFrame.from_dict({'arrival_time':[], 
                                 'ql':[],
                                 'les':[],
 #                                'lcs':[],
                                 'numBusy':[],
                                 'delay':[]})
for reps in range(0, 100, 1):

    SimFunctions.SimFunctionsInit(Calendar)#, TheQueues, TheCTStats, TheDTStats, TheResources])
    SimFunctions.Schedule(Calendar, "Arrival", NSPP())
    SimFunctions.Schedule(Calendar, "EndSimulation", RunLength)


    NextEvent = Calendar.Remove()
    SimClasses.Clock = NextEvent.EventTime
    
    Arr_list= [] #primary_input
    LES = []
    state = []
    QL = []
    delay = [] #Target
    
    les = 0
    
    if NextEvent.EventType == "Arrival":
        Arrival()
    
    while NextEvent.EventType != "EndSimulation":
        NextEvent = Calendar.Remove()
        SimClasses.Clock = NextEvent.EventTime

        if NextEvent.EventType == "Arrival":
            Arrival()
        elif NextEvent.EventType == "EndOfService":
            EndOfService()
    
    delete =100
    
    rep_data = {'arrival_time':Arr_list[delete:len(delay)], 
                'ql':QL[delete:len(delay)],
                'les':LES[delete:len(delay)],
                'numBusy':state[delete:len(delay)],
                'delay':delay[delete:]}
    rep_df = pd.DataFrame.from_dict(rep_data)
    mms_df = pd.concat([mms_df,rep_df])
    

mms_df['NI'] = np.mean(mms_df['delay'])
mms_df['ql_est'] = mms_df['ql']*(Mean_st/agent_pool_size)
mms_df['lcs'] = mms_df['les'].shift(1,fill_value=0)

mms_df['arr_pct'] = mms_df['arrival_time']/RunLength


mms_df['ql_2_avg'] = mms_df['ql'].rolling(window=2).mean().fillna(0)
mms_df['ql_5_avg'] = mms_df['ql'].rolling(window=5).mean().fillna(0)
mms_df['ql_10_avg'] = mms_df['ql'].rolling(window=10).mean().fillna(0)
mms_df['ql_20_avg'] = mms_df['ql'].rolling(window=20).mean().fillna(0)

mms_df['dh_2_avg'] = mms_df['delay'].rolling(window=2).mean().fillna(0)
mms_df['dh_5_avg'] = mms_df['delay'].rolling(window=5).mean().fillna(0)
mms_df['dh_10_avg'] = mms_df['delay'].rolling(window=10).mean().fillna(0)
mms_df['dh_20_avg'] = mms_df['delay'].rolling(window=20).mean().fillna(0)


mms_df.to_csv('mms_SimData.csv',index=False)





Results = pd.DataFrame(AllStats, columns=["Average Delay", 
                                          "Average Queue Length", 
                                          "Average number of busy Agents"])


print ("Delay CI", mean_confidence_interval(mms_df['delay'])) #do w/Numpy
print ("Queue Length CI", mean_confidence_interval(mms_df['ql']))#do w/Numpy
