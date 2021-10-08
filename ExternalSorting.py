# -*- coding: utf-8 -*-
"""
Created on Thu Mar  25 21:14:48 2021

@author: zeelp
"""

import pandas as pd
import numpy as np
import random
import math
import os

#Size of DataSet 
dataset_size=20    

#path of folder where data needs to be stored  
#Please change this value while running code                                                                                  
path_of_folder='C:\\Users\\zeelp\\Desktop\\New folder\\' 
      
temp_list_amt = []
total_no_of_files=0
ListOfFilesandCurrentRun = []
MaxLimit = 70000
Secondcaseflag = 0
number_of_files=0


#------------CAUTION:: This function deletes all files existing in folder-------------#

def DeleteExistingFiles():
    for f in os.listdir(path_of_folder):
        if not f.endswith(".txt"):
            continue
        os.remove(os.path.join(path_of_folder, f))

#------------This function generates files with given location name-----------------#

def FindFilePath(count):                 #funtion that creates files with 1.txt, 2.txt, etc.
        count=str(count)
        file_name=path_of_folder
        file_name+=count+'.txt'
        return file_name

#--------------------------dataset generated-----------------------------------------#    
#funtion generates dataset of given size and saves in OriginalDataset.txt file
def generateDataset():  
    name_length=3                       #length of Name
    def random_name():                  #generates random names of give size
        three_letter_words=""
        for i in range(0, name_length):
            three_letter_words+=chr(random.randint(97, 122))
        return three_letter_words                    
    
    lis=[]
    for i in range(dataset_size):
        lis.append(random_name())

    dataset = pd.DataFrame({
                     "TS"  : np.random.randint(1, 60000, size=dataset_size),
                     "Category": np.random.randint(low=1,high=1500,size=dataset_size)
                     })

    dataset.insert(0, 'T_ID', range(1, 1 + len(dataset)))   #index of transactions inserted at position 0
    dataset.insert(2, 'Name', lis)              #index of transactions inerted at position 2
    
    #saves dataframe at given location with name OriginalDataset.txt
    np.savetxt(FindFilePath('OriginalDataset'), dataset.values, fmt='%s')

#--------------------------------------------Gnenerates Intermidiate files-----------------------------#

def generateIntermmidiateFiles(no_records_in_each_file):
    number_of_files=math.ceil(dataset_size/no_records_in_each_file)
    num_records=no_records_in_each_file

    np_file=open(FindFilePath('OriginalDataset'),'r')
    Lines=np_file.readlines()
    
    count=1
    line_count=0

    for i in range(number_of_files):        
        num_records=no_records_in_each_file            
        file_path=FindFilePath(count)
        
        file=open(file_path, 'w')
    
        while(num_records>0):                   #if number of records to be entered in one file are > 0
            if(line_count<dataset_size):
                file.write(Lines[line_count])   #writes current line in currently open file
                line_count=line_count+1
        
            num_records=num_records-1
            
        if(i<(number_of_files-1)):              #writes path to next file at bottom of the current file
            file.write('->'+FindFilePath(count+1))
        else:
            file.write('->last file!')
        count=count+1
        file.close()

    np_file.close()

    
#--------------------------------------Sorts and Writes data back in files------------------------------------#

#Takes input str key, and returns int key
def ReturnInt(key):
    try:
        return int(key)
    except ValueError:
        return key

#Takes list as input and returns sorted list contaning tuples of amount and corresponding index
def sort_run(mm_list):
    temp_list_amt=[i.split(' ', 3)[1] for i in mm_list]         
    temp_dic={}                                                 
    for i in range (len(mm_list)):
        temp_dic[temp_list_amt[i]]=i
    
    return (sorted(temp_dic.items(), key= lambda t: ReturnInt(t[0]))) #returns sorted list contaning tuples of amount and corresponding index 

#Word of caution, this funtion will delete contents of 1.txt, 2.txt...files    
#This funtion writes records in files in sorted order, and appends
#file path of next file at the end, and if it is last file of that
#particular run, appends '->last file'  
  
def writeSorteddb(sorted_dictionary, mm_list, InitialFileCounter, last_file_counter, no_of_runs, num_of_rec_in_file):
    count=InitialFileCounter
    dictionary_counter=0
    run_complete_counter=0
    file=open(FindFilePath(count),'w')

    while(no_of_runs and count<=last_file_counter and dictionary_counter<len(sorted_dictionary)):
        
        index=sorted_dictionary[dictionary_counter][1]      #index of record with minimum transaction amount
        file.write(mm_list[index])                          #using index, fetch corresponding record from mm_list
        dictionary_counter=dictionary_counter+1
        run_complete_counter=run_complete_counter+1
    
        if(run_complete_counter==num_of_rec_in_file):
            no_of_runs=no_of_runs-1
            run_complete_counter=0
            count=count+1
            
            if(count<=last_file_counter):
                file.write('->'+FindFilePath(count))        #write filepath with starting characters as '->'
                
            file.close()
            
            if(no_of_runs>0 and count <= last_file_counter):    #if not the last file, open that file and runs are also > 0
                file=open(FindFilePath(count),'w')
    
    file=open(FindFilePath(last_file_counter),'a')  #writing 'last file' at the end of last file of a particular run
    file.write('->last file')
    file.close()

#-----------------------------------------Generates Final Output file----------------------------------------#

#This Function generates Final file named as ExternallySortedDataset.txt
#containing records in sorted order.

def GenerteFinalFile(total_no_of_files):
    i=0
    global Secondcaseflag
    
    finalfile = open(FindFilePath('ExternallySortedDataset'),'a')
    while(i<total_no_of_files): 
        if(Secondcaseflag == 0):
            filenameis = path_of_folder + "Outputrun" + str(i) + '.txt'
            file = open(filenameis,'r')
            linesinfile = file.read()
            finalfile.write(linesinfile)
            file.close()
            i=i+1
            
        else:
            
            filenameis = path_of_folder + str(i+1) + '.txt'
            file = open(filenameis, 'r')
            line_count = sum(1 for i in open(filenameis))
            linesinfile = file.readlines()[:line_count-1]
            for record in linesinfile:
                finalfile.write(record)
            
            file.close()
            
            i=i+1
            
    finalfile.close()

#--------------------------------------------Return Transaction Amount------------------------------------------#

#This Function takes input a string and returns 1 value of it, i.e. Transaction Amount.
def FindTransactionAmount(recordtr):
    
    #Record String delimited by ' '
    record = recordtr.split(' ') 
     
    return record[1]

#-----------------------------------------Finds records containing Minimum Value----------------------------------#

#This function return record containing minimum transaction amount, run number 
#associated with that record

def FindLeastValue(mainMemory,indexWithinRuns):
    
    minValue = MaxLimit
    size = len(indexWithinRuns)
    minindex = -1
    index = 0
    record = ""
    
    #if minimum value of transaction amount is found, update minvalue by 
    #that minimum value
    for RunNumber in range(0, size):
        
        index = indexWithinRuns.get(RunNumber)[0]
        
        if(int(FindTransactionAmount(mainMemory[index])) < minValue):
            minValue = int(FindTransactionAmount(mainMemory[index]))
            record = mainMemory[index]
            minindex = RunNumber
		
    return [record, minindex]
    

#----------------------------------Initializing Main Memory---------------------------------------------------------#

def InitializeMainMemory(MainMemoryDatablock, SizeOfDatablock):
    InitialMainMemory = []
    runindex = 0
    

    for dbcounter in range(0, MainMemoryDatablock):
        
        #Name of First file of every run
        NameOfFile = ListOfFilesandCurrentRun[runindex][0]
        inputfileinstance = open(NameOfFile)
        
        #If file is successfully opened, read the whole content of file
        #untill you reach '->'
        
        if(os.path.exists(NameOfFile)):
            while(True):
                currentline = inputfileinstance.readline().strip()
                if currentline[0:2] != '->':
                    InitialMainMemory.append(currentline)
                else:
                    break
            runindex = runindex + 1
            inputfileinstance.close()
    
    #returning initialized mainmemory
    return InitialMainMemory

#-----------------------------------K-Way MERGE FUNTION----------------------------------------------------#

def KwayMergeFunction(no_runs, sizeMainMemory, totalCounter, SizeOfDatablock):
    
	dataBlocksInMM = min(no_runs,sizeMainMemory-1)
    
    #Representation of main memory
	ListOfMainMemory = []                               
	totalRecords = dataset_size
    
    #Initializing main memory by first files of every runs
	ListOfMainMemory = InitializeMainMemory(dataBlocksInMM,SizeOfDatablock)
	
    #Counts the number of output files
	outputFileCounter = 0  

    #This list Contains Datablock, when it is full, it will be written in file.
	outputDataBlock = []
    
    #Dictionary containing int indicating run number and list of ints
    #containing current file in location 0, and last file of current file in location 1
	indexWithinRuns = {}   
    
    #contains an record string (returned from FindLeastValue) at location 0, 
    #and corresponding run index     
	minValuePair = []

    #Fixing the upper limts of runs
    #for example: {0: [1, 2], 1: [3, 5], 2: [6, 8], 3: [9, 11]}
    # files of run 0, will lie in range of [0,2] but here current file is 1.txt, 
    #files of run 1 will lie in range [3,5] and current file is 3.txt
    
	for i in range(0, dataBlocksInMM):
	
	    indexWithinRuns[i] = [i*SizeOfDatablock, SizeOfDatablock*(i+1)-1]

    #This loop ends if number of outputfile and total number of files becomes equal
	while(outputFileCounter != totalCounter):
        
            #if outputdatablock if full, then do not enter this loop
            while(len(outputDataBlock) != SizeOfDatablock and totalRecords > 0):
                
                    minValuePair = FindLeastValue(ListOfMainMemory, indexWithinRuns)
                    
                    if(int(FindTransactionAmount(minValuePair[0])) == MaxLimit):
                        pass
                    
                    outputDataBlock.append(minValuePair[0])
                    totalRecords = totalRecords - 1
                    (indexWithinRuns[minValuePair[1]])[0] = (indexWithinRuns[minValuePair[1]])[0] + 1
                    
    
                    if( ((indexWithinRuns[minValuePair[1]])[0]) > ((indexWithinRuns[minValuePair[1]])[1])):
        
                        runFileName = ListOfFilesandCurrentRun[minValuePair[1]][0]
                    
                        #IndexPos used for reinitializing runs and maim memory
                        indexPos = minValuePair[1] * SizeOfDatablock
                        
                        #Initially filling garbage values in main memory for current run
                        for i in range(0, SizeOfDatablock):
                            invalidString = str(MaxLimit)+" "+str(MaxLimit)+" "+"NULL"+" "+"NULL"
                            ListOfMainMemory[indexPos + i] = invalidString
                      
                        #if path of file is valid, then next file exits in current run.
                        if(os.path.exists(runFileName)):
                            with open(runFileName) as file:
                                line = file.readline().strip()
                                lastline = file.readlines()[-1]
                                runFileName = lastline[2:]
                                
                            file.close()
                        
                        indexPos = minValuePair[1] * SizeOfDatablock
                        if(runFileName != '->last file' and runFileName != 'last file'):
                            ListOfFilesandCurrentRun[minValuePair[1]] = [runFileName,indexPos]
                            fileInstance = open(runFileName)
                        
                  
                            #reinitializing indexWithinRuns
                            (indexWithinRuns[minValuePair[1]][0]) = indexPos
                        
                            #If next file exists, mainmemory is overwritten by its data
                            #Reading Datablock into main memory.
                            if(os.path.exists(runFileName)):
                                while(True):
                                    line = fileInstance.readline().strip()
                                    if line[0:2] != '->': 
                                        ListOfMainMemory[indexPos] = line
                                    else:
                                        break
                    
                                    indexPos = indexPos + 1
                            fileInstance.close()
            
                          
                        else:
                            #reinitializing indexWithinRuns
                            #No need to Update mainmemory since, invalid values are already filled in the begining
                            (indexWithinRuns[minValuePair[1]][0]) = minValuePair[1] * SizeOfDatablock
                            
    
                          
            #Files containing final output data of Datablock size ex: Outputrun1.txt            
            outputfilename = path_of_folder + "Outputrun" + str(outputFileCounter) + ".txt"
            outputfileinstance = open(outputfilename, 'w+')
            
            
            #Writing Output sorted datablock in file
            i = 0
            while(i < len(outputDataBlock)):
                outputfileinstance.write(outputDataBlock[i])
                outputfileinstance.write('\n')
                i = i + 1  
            
            #Output datablock is cleared as, new records can come in main memory
            outputDataBlock.clear()
            outputFileCounter = outputFileCounter + 1
    
                  
                

#---------------------------------External Sorting-----------------------------------------#

def ExternalSorting(mm_in_terms_of_db, num_of_rec_in_file):
    mm_in_terms_of_rec = mm_in_terms_of_db * num_of_rec_in_file
    no_of_runs = math.ceil(dataset_size / mm_in_terms_of_rec)
    
    print('num of runs: ', no_of_runs)

    #if main memory is greater than no of runs
    if mm_in_terms_of_db > no_of_runs:  
        print('Case 1: Main Memory size is Greater than Number of Runs')
        #current run counter
        current_run = 1
        
        #contains first file Counter of specific run
        InitialFileCounter=1
        count=1
        
        #representation of main memory in mm_list
        mm_list=[]     
        dictionary={}
        batch_file_counter = 0
        no_of_files = math.ceil(dataset_size / num_of_rec_in_file)
        global total_no_of_files
        
        #original number of runs are retained here
        no_of_runs_copy = no_of_runs
        
        while(no_of_runs):
                #open file with name as that of count at specified path
                file=open(FindFilePath(count))
                
                #reads one line fr
                line = file.readline()
                while(line[0] != '-'):
                    #if not last line, append it to mm_list
                    mm_list.append(line)
                    
                    #read new line from file
                    line=file.readline()
                    
                file.close()
                
                #counter of file increased
                count=count+1
                
                batch_file_counter = batch_file_counter+1
                
                no_of_files=no_of_files-1
                
                total_no_of_files = total_no_of_files + 1
                
                #will enter this flow only when one run is completed!
                if(batch_file_counter == mm_in_terms_of_db or no_of_files<1 ):
                    dictionary=sort_run(mm_list)
                    
                    #Filepath of first file of every run is appended in ListOfFiles and current run
                    ListOfFilesandCurrentRun.append([FindFilePath(InitialFileCounter), (current_run - 1)*num_of_rec_in_file ])
                    
                    #Writes sorted datablocks in files, that is sorted files wrt to each run are generated in this fuction.
                    writeSorteddb(dictionary, mm_list, InitialFileCounter, count-1 , mm_in_terms_of_db, num_of_rec_in_file)
                    
                    #Initial File counter plus Datalock size, that is counter of first file of next run
                    InitialFileCounter=InitialFileCounter+mm_in_terms_of_db
                    
                    #Dictionary in cleared, so that new data can come in it
                    dictionary.clear()
                    
                    #Dictionary in cleared, so that new data can come in it
                    mm_list.clear()
                    
                    batch_file_counter = 0
                    
                    current_run = current_run + 1
                    
                    no_of_runs=no_of_runs-1
        
        
        #Now Merge function will be called!
        KwayMergeFunction(no_of_runs_copy, mm_in_terms_of_db, total_no_of_files, num_of_rec_in_file)
    
    else:
        print('Case 2: Main Memory size is less than or equal to Number of Runs')
        total_no_of_files = math.ceil(dataset_size / num_of_rec_in_file)
        global Secondcaseflag
        Secondcaseflag = 1
        
 
        
#--------------------------------------Code Execution starts------------------------------------------#

print('::::Data is in Format::::')
print('Transaction ID, Transaction sales amount, Customer name, Category of item')

#This Function call deletes existing files in folder specified in path_of_folder
DeleteExistingFiles()

#This Function Generates Random Dataset consisting of Data in format
#Transaction ID, Transaction sales amount, Customer name, Category of item
generateDataset()
print('dataset generated successfully!')

#B is DataBlock size
B=int(input('Enter number of records in each block:'))

#This funtion call generates Intermediate Files which will contain records of size B 
generateIntermmidiateFiles(B)

#M is the size of Main Memory in terms of B(DataBlock size)
M=int(input('Enter main memory size (in terms of number of disk blocks):'))

#ExternalSorting Funtion Called here
ExternalSorting(M,B)

#This Funtion Generates Final Output File
GenerteFinalFile(total_no_of_files)

print('External Sorting Completed and Final Sorted Data is saved in ExternallySortedDataset.txt')
