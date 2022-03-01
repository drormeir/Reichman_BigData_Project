import sqlite3
import threading
import pandas as pd
import os
import time

class MapReduceEngine():
    '''Class for implementing MapReduce'''

    @staticmethod    
    def execute(input_data, map_process_creator, shuffle_read_temp_from_input, reduce_process_creator, max_threads=8):
        '''Function to execute the logic of MapReduce'''
        #run mapping
        start_time = time.time()
        num_threads = MapReduceEngine.run_threads("Map", input_data, map_process_creator, max_threads)
        conn   = sqlite3.connect('temp.db')
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS temp_results
                      (key TEXT, value TEXT)''')
        conn.commit()
        for i in range(num_threads):
            data = shuffle_read_temp_from_input(i)
            #use SQL to save results of mapping to database
            data.to_sql('temp_results', conn, if_exists='append',index=False)
        cursor = conn.cursor()
        #get results of mapping in list
        cursor.execute('SELECT key, GROUP_CONCAT(value) FROM temp_results GROUP BY key ORDER BY key ASC')
        rows = cursor.fetchall()
        conn.close()
        os.remove('temp.db')
        #run reduce logic
        MapReduceEngine.run_threads("Reduce", rows, reduce_process_creator, max_threads)
        end_time = time.time()
        print("MapReduce Completed in {} seconds.".format(end_time - start_time))
        return
    
    def run_threads(name, input_objects, process_function, max_threads):
        start_time    = time.time()
        input_len     = len(input_objects)
        num_threads   = min(input_len,max_threads)

        print("Starting {} stage with {} input objects splitted to {} threads...".format(name,input_len,num_threads))
        
        if num_threads > 1:
            # create threads
            split_size    = input_len // num_threads
            split_residue = input_len % num_threads
            threads_vec   = []
            for ind in range(num_threads):
                if ind < split_residue:
                    start = ind*(split_size+1)
                    end   = (ind+1)*(split_size+1)
                else:
                    start = split_residue*(split_size+1) + (ind-split_residue)*split_size
                    end   = start+split_size

                args = (name, ind, process_function, input_objects[start:end])
                threads_vec.append(threading.Thread(target=MapReduceEngine.run_thread, args=args))
            for t in threads_vec:
                t.start()
            #wait for threads to finish
            for t in threads_vec:
                t.join()
        else:
            MapReduceEngine.run_thread(name, 0, process_function, input_objects)
        end_time = time.time()
        print("{} stage completed in {} seconds.".format(name,end_time - start_time)) 
        return max(abs(num_threads),1)
    
    def run_thread(name, threadID, process_function, input_objects):
        print("{} thread {} is starting with {} objects ...".format(name, threadID, len(input_objects)))
        process_function(threadID, input_objects)
        print("{} thread {} is completed".format(name, threadID))
        return
        
