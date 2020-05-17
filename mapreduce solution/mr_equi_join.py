from mrjob.job import MRJob 
from mrjob.compat import jobconf_from_env
import sys
import time 

R1_NAME = None 
R2_NAME = None 

class MREQuiJoin(MRJob):

    def configure_args(self):
        """
        This method configures commandline 
        options for the program. 
        The commandline options:
            --column: the index of the join column.
        """
        super(MREQuiJoin, self).configure_args()
        self.add_passthru_arg(
            "--column", 
            help = "specify the index of the join column. "
        )

    def mapper_init(self):
        """
        This method initializes or setups 
        the resources needed by the mapper function. 
        """
        self.table_delim_map = "|"
        self.column = int(self.options.column)

    def mapper(self, _, line):
        try:
            #Make tables names global
            global R1_NAME 
            global R2_NAME

            #Get the names of the tables. 
            input_file_name = jobconf_from_env('map.input.file')

            #Augment file name to line 
            line = input_file_name + "*" + line 

            #store the names of the tables.
            if input_file_name is not None:
                if R1_NAME is None:
                    R1_NAME = input_file_name 
                else:
                    if R1_NAME != input_file_name:
                        R2_NAME = input_file_name

            #get join column 
            key = line.split(self.table_delim_map)[self.column-1]
            yield key, line 

        except:
            print("Something is not right.")
            sys.exit(-1) 

    def reducer_init(self):
        """
        This method initializes or setup 
        the resources needed by the reducer function. 
        """
        self.table_delim_red = "|" 

    def reducer(self, key, values):
        r1  = [] ##table one
        r2 = [] #table two 

        #Step through tuples of tables
        #Get the name of the table (the first string when `value` is split)
        #Add values to the r1(table 1) and r2(table 2) base on the table name.
        c = 0
        for value in values:
            c +=1
            table_name = value.split("*")[0]
            if table_name == R1_NAME:
                r1.append(value.split("*")[1])
            elif table_name == R2_NAME:
                r2.append(value.split("*")[1])
            else:
                print('No table name')
                
        #Get command lin options
        column = int(self.options.column) - 1

        #Do equi-join
        for i in range(len(r1)):
            for j in range(len(r2)):
                r2_tuple = r2[j].split(self.table_delim_red)
                r2_tuple.pop(column)
                joined_tuple = r1[i] + "|".join(r2_tuple)

                yield None, joined_tuple

if __name__=="__main__":
    t1 = time.time()
    MREQuiJoin.run()
    t2 = time.time()
    print("Elapsed time =", t2 - t1)