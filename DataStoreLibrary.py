import threading 
from threading import Thread 
import time
import sys
import os.path
import json

#create operation
lock = threading.RLock()

def create(key,value,timeout=0,filepath="dataStore.json"):

    '''
    This function creates a json-object if key doesn't exists in the json-file
    If key is present error message is returned
    Appropriate thread safe lock is also implemented
    Input: key, value, time_to_live, filepath
    Output: creates json-object in json file 

    '''
    with lock:
        with open(filepath,'a+') as json_file:
            json_file.seek(0)
            if os.stat(filepath).st_size != 0:
                data = json.load(json_file)
            else:
                data = {}#dictionary to store data
            v=sys.getsizeof(value)
                
            if not(os.stat(filepath).st_size == 0) and key in data:
                if data[key][1]!=0 and time.time()>=data[key][1]:
                    del data[key]
                    json_file.seek(0)
                    json_file.truncate()
                    json.dump(data, json_file, indent=4)
                    # json_file.close()
                    create(key,value,timeout,filepath)
                else:
                    print("ERROR: KEY ",key," ALREADY EXISTS")
            else:
                if(key.isalpha()):
                    if (len(data)<(1024*1020*1024) and v <= (16*1024*1024)): #file and Json-object size constraints
                        if timeout==0:
                            l=[value,timeout]
                        else:
                            l=[value,time.time()+timeout]
                        if len(key)<=32: #key_name capped at 32chars
                            data[key]=l
                            print("KEY ",key," CREATED SUCCESSFULLY!!")
                        else:
                            print("ERROR: KEY ",key," SIZE EXCEEDED 32 CHARACTERS")
                        
                    else:
                        print("ERROR: MEMORY LIMIT EXCEEDED!!")
                else:
                    print("ERROR: ENTER VALID KEY_NAME(only char)")
            json_file.seek(0)
            json_file.truncate()
            json.dump(data, json_file, indent=4)
            json_file.close()

#read operation
           
def read(key,filepath):

    '''
    This function reads the json-object if key exists in the json file
    If key is not present error message is returned
    Appropriate thread safe lock is also implemented
    Input: key, filepath
    Output: Prints the key-value pair from the json file for the key
    
    '''
    with lock:
        with open(filepath,'a+') as json_file:
            json_file.seek(0)
            if os.stat(filepath).st_size != 0:
                data = json.load(json_file)
            else:
                data = {}

            if key not in data:
                print("ERROR: ENTER VALID KEY_NAME ",key)
            else:
                b=data[key]
                if b[1]!=0:
                    if time.time()<b[1]: #comparing the time
                        #string=str(key)+":"+str(b[0]) #to return the value in the format of Json-Object
                        print(key,":", data[key])
                    else:
                        del data[key]
                        print("ERROR: TIME TO ACCESS THE",key,"HAS EXPIRED")
                else:
                    print(key,":", data[key])
            json_file.seek(0)
            json_file.truncate()
            json.dump(data, json_file, indent=4)
            json_file.close()

#delete operation

def delete(key,filepath):

    '''
    This function deletes the json-object if key exists in the json file
    If key is present error message is returned
    Appropriate thread safe lock is also implemented
    Input: key, filepath
    Output: deletes the key-value pair for the key 
    
    '''
    with lock:
        with open(filepath,'a+') as json_file:
            json_file.seek(0)
            if os.stat(filepath).st_size != 0:
                data = json.load(json_file)
            else:
                data = {}
                
            if key not in data:
                print("ERROR: ENTER VALID KEY_NAME",key)
            else:
                b=data[key]
                if b[1]!=0:
                    if time.time()<b[1]:#comparing time
                        del data[key]
                        print("KEY ",key,"DELETION COMPLETED")
                    else:
                        del data[key]
                        print("ERROR: TIME TO ACCESS THE",key,"HAS EXPIRED")
                else:
                    del data[key]
                    print("KEY ",key," DELETION COMPLETED")
            json_file.seek(0)
            json_file.truncate()
            json.dump(data, json_file, indent=4)
            json_file.close()