from os.path import exists  # need to check if file exists on OS path
import os
import pypyodbc as pyodbc
import re
import time
#import sys
#------------------------------------------------------------------------------------------------------

# -- Temporary basic setup for user input
#path = input("Enter file path: ")
path = "Z:\\Transitory_Docs(temp)\\Azam\\Rao\\"
print ("path is = " + path)

if exists(path):
    print("Valid path")
    #os.chdir(path)
    dir_list = os.listdir(path)
else:
    print("ERROR: Path NOT Found")
    exit()

print("Files in the folder are:")
print("-" *80)
    
for filename in dir_list:
    print(filename)


filename = input("Enter full filename with extension: ")
#------------------------------------------------------------------------------------------------------


# -- Global declarations
obj_touched_list = []  # list to collect table, view objects touched in the script file.  will be used to fetch metadata from audit control table.

#file = open(filename, encoding="utf8")
f3 = open(path+filename[:-4]+"_final.txt", 'w')
#------------------------------------------------------------------------------------------------------




def open_file(path, filename):
    #print ("File exists? %r" %exists(path+filename))
    if exists(path+filename):
        file = open(path+filename, "r")
        indata = file.read()
        print("-"*80)        
        print("Processing file: "+path+filename)
        print ("File size in bytes = %d" % len(indata))
        file.seek(0)
        print("="*80)  # End of function marker
        return file
    else:
        print ("ERROR: File not found, Please correct and run again")
        exit()
        #sys.exit
    

# clean_file function will clean the script file of all extra lines that are not required to get Table/view, column names.
# clean up each line of the DDL file that starts with keywords CREATE, ALTER, DROP, REPLACE and ends with ; Then read each line as a statement and parse in a standard way.
# skip the single line and multi-line comments statements.
def clean_file(path, filename):
    #f1 = open(path+filename, 'r')
    f1 = open_file(path,filename)
    f2 = open(path+filename[:-4]+"_outfile.txt", 'w')

    print("--- Cleaning input file -- "+filename[:-4]+" ---")
    for line in f1:
        line = line.strip()+"\n"
        #print(line)
        if (line[0:2] == "--"):  # single line comments
            #print ("Comments line found")
            f2.write("")
        elif (line.find("/*") >=0):  # Multiline comments
              #print("Multiline comments starts")
              while (line.find("*/") < 0):
                  #print("Skip line: "+line)
                  line = next(f1)  # skip lines between Multiline comments marker /* and */
        elif (line[0:7] == "INSERT "):
            while(line.find(";") < 0):
                line = next(f1)  # skip lines for INSERT statement till statement ends with a semicolon ;
        elif (len(line) > 1) and (line != "ACCESS\n") and (line != "LOCKING ROW\n") and (line.find("NO BEFORE JOURNAL") <0) and (line.find("NO AFTER JOURNAL") <0) and (line.find("CHECKSUM = DEFAULT") <0) and (line.find("DEFAULT MERGEBLOCKRATIO")<0):
            #print("last character is = " + line.strip()[-1:])

            if (line.find("TITLE ")>=0): line = line[:line.find("TITLE ")]+",\n"  #ignore string starting with the keyword "TITLE"

            line = re.sub(' +',' ',line)  # a simple way to remove multiple spaces in a string

            if (line.find("COMPRESS ")>=0):  # to remove COMPRESS keyword and the string followed by it.  Add comma and line feed to keep the syntax
                line = line[:line.find("COMPRESS ")]+","+"\n"  # ignore string starting with the keyword "COMPRESS"

            # UPPER case the whole line and strip spaces around it before writing it to outfile
            #f2.write(line.upper().strip())   
            f2.write(line.upper())  
            
            if (line.strip()[-1:] == ";"):
                f2.write("\n")
            #f2.write(line.replace(txt, ''))
    print("--- Cleaned file written to "+filename[:-4]+"_outfile.txt ---")
    print("=" *80)  # End of function marker
    f1.close()
    f2.close()



def ddl_type(line):
    line = line.strip()
    #print(line)
    #print(line[0:6].upper())
    if (len(line) == 0):
        #print("Blank line")
        return 0
    if (line[0:4].upper() == "DROP"):
        #print ("Drop statement")
        return 1
    elif(line[0:6].upper() == "CREATE"):
        if (line.find("CREATE VIEW ")>=0):  # view statement may start with CREATE VIEW instead of REPLACE VIEW, return code 7 for CREATE VIEW
            return 7
        if (line.find("CREATE JOIN INDEX")>=0):  # if it is a CREATE JOIN INDEX statement, return code 8
            return 8
        #print("Create statement")
        return 2
    elif (line[0:5].upper() == "ALTER"):
        #print("Alter statement")
        return 3
    elif(line[0:7].upper() == "REPLACE"):
        #print("Replace statement")
        return 4
    elif(line[0:2] == "--"):
        return 5
    else:
        #print("Unknown non-blank Statement")
        return 6


def parse_create_table_script(line):
    line = line.upper()
    #print("Line passed for Create Table = " + line)
    ### this string "col_names" MUST be clear of any COMPRESS keywords with its 3 scenarios, 1. "COMPRESS ," 2. "COMPRESS COLNAME,", 3. "COMPRESS (COL1, COL2),"
    '''
    # is it CREATE TABLE or CREATE VIEW
    if (line.find(" TABLE ")>=0):    # its a CREATE TABLE statement
        t_dbname = line[line.find(" TABLE ")+7:line.find(".")]  # database name is after the keyword TABLE and before the dot .
    elif (line.find(" VIEW ") >=0):  # its a CREATE VIEW statement
        t_dbname = line[line.find(" VIEW ")+6:line.find(".")]  # database name is after the keyword TABLE and before the dot .
    '''
        
    #print("Line passed to create table script: "+line)
    t_dbname = line[line.find(" TABLE ")+7:line.find(".")]  # database name is after the keyword TABLE and before the dot .
    tablename = line[line.find(".")+1:line.find("(")]       # table name is after the dot . and before open parenthesis (
                                                            # assuming script file does NOT have these keywords,NO FALLBACK ,     NO BEFORE JOURNAL,     NO AFTER JOURNAL,     CHECKSUM = DEFAULT,     DEFAULT MERGEBLOCKRATIO
    if (tablename.find("NO FALLBACK") >=0):
        tablename = tablename[:tablename.find("NO FALLBACK")]
        tablename = tablename[:tablename.find(",")]         # there must be a comma before NO FALLBACK keyword, trim it.
    print(t_dbname+"."+tablename)
    #get_audit_columns(t_dbname, tablename)                 # get the table and column names from Audit Control Table
    obj_touched_list.append(t_dbname.strip()+"."+tablename.strip())         # add each object touched to a list.  It will be used to fetch data from Audit Control Table 
    #f3.write(t_dbname+"."+tablename+"\n")

    col_names = line[line.find("("): line.find(";")]        # column names are after the first open ( and before end of statement with a semicolon ;
    #col_names = line[line.find("("): ]        # column names are after the first open ( and before end of statement with a semicolon ;

    #print("Table column names befor removing first and last character are: ")
    #print(col_names)
    #time.sleep(5)


    col_names = col_names[1:-1]                             # removing first and last character of ( and )

    col_names = col_names.split(",")                        # split the string by comma to a list
    #print(col_names)
    
    for colname in col_names:  # print each column and datatype from the list as dbname.tablename.columnname datatype
        if (colname.find("CHARACTER SET LATIN NOT CASESPECIFIC")>=0): colname = colname[:colname.find("CHARACTER SET LATIN NOT CASESPECIFIC")]  # remove keywords CHARACTER SET LATIN NOT CASESPECIFIC from column name
        print(t_dbname.strip()+"."+tablename.strip()+"."+colname.strip())
        f3.write (t_dbname.strip()+"."+tablename.strip()+"."+colname.strip()+"\n")
    print("="*80)  # End of function marker


def parse_alter_table_script(line):
    print("-- Alter table statement found --")
    print(line)
    line = line.upper()
    t_dbname = line[line.find(" TABLE ")+7:line.find(".")]
    tablename = line[line.find(".")+1:]
    tablename = tablename[:tablename.find(" " )]
    print("--- Database.Tablename ---")
    print(t_dbname+"."+tablename)

    obj_touched_list.append(t_dbname.strip()+"."+tablename.strip())         # add each object touched to a list.  It will be used to fetch data from Audit Control Table 
    col_names = line[line.find(tablename)+len(tablename)+1: line.find(";")]  # column names on the same line
    col_names = col_names.split(",")
    #print(col_names)  # to print list of columns names

    print("--- Database.Table.Column ---")
    for colname in col_names:
        colname = colname.strip()                # to remove any spaces in the beginning of line
        colname = colname[colname.find(" ")+1:]  #ignore keywords like ADD, DROP, MODIFY, RENAME and get the column name after first space
        if (colname.find(" TO ") >=0): colname = colname[colname.find(" TO ")+4:]
        colname = colname.strip()                # there may be more than one spaces between the keyword and column name
        
        print (t_dbname.strip()+"."+tablename.strip()+"."+colname.strip())
        f3.write(t_dbname.strip()+"."+tablename.strip()+"."+colname.strip()+"\n")
    print("-- End of Parse Alter statement--")
    print("="*80)  # End of function marker


def parse_drop_table(line):
    if (line.find("JOIN INDEX") > 0):
        t_dbname = line[line.find("JOIN INDEX")+10:line.find(".")]
    else:
        t_dbname = line[line.find(" TABLE ")+7:line.find(".")]  # pick up databasename where keyword 'TABLE' ends till it finds a dot (.)
    tablename = line[line.find(".")+1:line.find("(")]
    tablename = line[line.find(".")+1:line.find(";")]
    obj_touched_list.append(t_dbname.strip()+"."+tablename.strip())
    return(t_dbname.strip()+"."+tablename.strip())


def parse_view_script(line):
    #print("View script line passed: "+line)
    pre_selected_view_cols_ind = 0
    v_dbname = line[line.find(" VIEW ")+6:line.find(" AS ")]

    if (v_dbname.find("(") >=0):  # scenario when Replace view script is like REPLACE VIEW DB.TEST ( COL1, COL2) AS SELECT COL1, COL2 FROM DB.TEST2;
        v_dbname = v_dbname[:v_dbname.find("(")]
        #print("Columns names are given in script: "+line)
        v_colnames = line[line.find("(")+1:line.find(")")]
        pre_selected_view_cols_ind = 1
        #print(v_colnames)

    if (v_dbname.find(" ") >=0):  # scenario when Replace view script is like REPLACE VIEW DB.TEST COL1, COL2 AS SELECT COL1, COL2 FROM DB.TEST2;
        v_dbname = v_dbname[:v_dbname.find(" ")]
    print("-- View Name")
    print (v_dbname)
    
    obj_touched_list.append(v_dbname.strip())
    if (pre_selected_view_cols_ind == 0): v_colnames = line[line.find(" SELECT ")+8: line.find(" FROM ")]
    print("view column names befoer comma split are = " + str(v_colnames))
    print(v_colnames)  # view column names
    print("-- View column names ")
    v_colnames = v_colnames.split(",")

    ## test modification on 08092017
    print("View column names are: \n")
    print(v_colnames)
    ## end test

    
    for colname in v_colnames:
        if (colname.rfind(".") >= 0):  # column names is suffixed with tablename or dbname
            #print("extended col name = " +colname)
            colname = colname[colname.rfind(".")+1:]

        ## test modification on 08092017
        if (colname.find("END ") >=0):
            print("Found error row: ", colname)
            colname = colname[colname.find("END ")+4:]
            print("corect col name is = ", colname)
        ## end test

        
        if (colname.find(" END ") >=0): colname = colname[colname.find(" END ")+5:]  # column names with key END in it.  this would be applicable to CASE and END pair.
        if (colname.rfind(" AS ") >= 0):
            colname = colname[colname.rfind(" AS ")+4:]
            #print ("Column found with AS = ", colname)
        else:
            if (colname.find("CASE WHEN ")>=0):
                print("Column name with CASE: "+colname)
                colname = colname[colname.find("END ")+5:]
        #print(colname.strip())
        print (v_dbname.strip()+"."+colname.strip())
        f3.write(v_dbname.strip()+"."+colname.strip()+"\n")
    print("-- End of View parsing")
    print("=" *80)  # End of function marker

# Reads each line from outfile.txt and writes to final.txt file.
def get_table_cols(line):
    if (ddl_type(line) == 0):
        print("Blank line found, do nothing")
        f3.write("Blank line found, do nothing")
    elif(ddl_type(line) == 1):
        #Drop table statement, get the table name only.  What happens in columns_master in this case?  It probably retains anything sent in the classification.
        print ("DROP Statement: " + parse_drop_table(line))  #01162017
        f3.write("-- Below DROP TABLE statement below:\n")
        f3.write(parse_drop_table(line)+"\n")
        f3.write("-- End of DROP TABLE statement \n")
    elif(ddl_type(line)==2):
        #Create table statement
        print (parse_create_table_script(line))
    elif(ddl_type(line)==3):
        #Alter table statement
        #t_dbname = line[line.find(" TABLE ")+7:line.find(".")]
        #tablename = line[line.find(".")+1:]
        #tablename = tablename[:tablename.find(" " )]
        #print(t_dbname+"."+tablename)
        parse_alter_table_script(line)
    elif(ddl_type(line)==4 or ddl_type(line)==7):
        #Replace view statement
        #v_dbname = line[line.find(" VIEW ")+6:line.find(" AS ")]
        #print (v_dbname)
        #v_colnames = line[line.find(" SELECT ")+8: line.find(" FROM ")]
        #print("v_colnames = " + str(v_colnames))
        #print(v_colnames)  # view column names
        parse_view_script(line)
        pass
    elif(ddl_type(line) == 5):
        #comment line
        print("comments line found")
    elif(ddl_type(line) == 6):
        #un-blank statement, possible column names
        col_names = line[line.find(" ")+1: line.find(";")].strip()
        #print("ddl type 6: " + col_names)
    elif(ddl_type(line) == 8):  # if CREATE JOIN INDEX statement, do nothing.
        pass


#---------- Function to get details from Teradata database
def get_audit_columns(dbname, tbl_view_name):

    cnxn = pyodbc.connect('DSN=EDWPROD')  # connect to Teradata Production using user DSN (username, password is stored in DSN)
    cursor = cnxn.cursor()
    cursor.execute("select column_name from P_AUDIT_CONTROL_V_I.COLUMN_MSTR  where databasename = '" + dbname + "' and tbl_vw_name = '" + tbl_view_name + "'")

    print("Number of columns = " + str(cursor._NumOfCols()))
    print("Number of rows    = " + str(cursor._NumOfRows()))
    #print(cursor.rowcount)  # another way to print Number of Rows 

    #print(cursor.description)
    # -- display column names using loop below
    for d in cursor.description:
        print(d[0])

    if (cursor._NumOfRows() == 0):
        print("Object does not exist in Column Master table")
    print("-"*80)

    # print all rows of cursor
    rows = cursor.fetchall()
    #print (type(rows))
    #print (rows)

    for i in rows:
        #print(type(i))
        #print(i)
        colname = list(i)[0].strip()
        print(dbname + "." + tbl_view_name + "." + colname)

    cursor.close()
    cnxn.close()


def get_confidential_list():

    print("Connecting to Teradata column master table, please wait ...")
    cnxn = pyodbc.connect('DSN=EDWPROD')  # connect to Teradata Production using user DSN (username, password is stored in DSN)
    cursor = cnxn.cursor()
    cursor.execute("select distinct column_name, classification from P_AUDIT_CONTROL_V_I.COLUMN_MSTR where upper(classification) = 'CONFIDENTIAL' order by 1;")
    print("Confidential column names = " + str(cursor._NumOfRows()))

    f = open(path+"confidential_list.txt", 'w')
    rows = cursor.fetchall()
    for i in rows:
        colname = list(i)[0].strip()
        print(colname, file=f)
    f.close()
    cursor.close()
    cnxn.close()


    

#--- Read column master table and save it in file *column_master.txt
def file_audit_columns(obj_list):
    print("="*80)
    print("-- Connecting to Teradata column master table, please wait ...")
    cnxn = pyodbc.connect('DSN=EDWPROD')  # connect to Teradata Production using user DSN (username, password is stored in DSN)
    cursor = cnxn.cursor()

    f4 = open(path+filename[:-4]+"_column_master.txt", 'w')
    print("-- CONNECTED, Now Reading Column Master production table ...")
    # the below loop will write the column master file (Databasename.ObjectName.ColumnName) for each object in the DDL file.
    for obj in obj_list:
        dbname = obj[:obj.find(".")].strip()
        tbl_view_name = obj[obj.find(".")+1:].strip()
        #print(dbname + "." + tbl_view_name, file=f4)  -- Write Databasename.Tablename in column master file followed by Databasename.Tablename.ColumnName
        cursor.execute("select column_name from P_AUDIT_CONTROL_V_I.COLUMN_MSTR  where databasename = '" + dbname + "' and tbl_vw_name = '" + tbl_view_name + "'")

        if (cursor._NumOfRows() == 0):
            print("Object NOT FOUND in Column Master: "+dbname+"."+tbl_view_name)
            #print("-"*80)
        else:
            # print all rows of cursor
            rows = cursor.fetchall()
            for i in rows:
                colname = list(i)[0].strip()
                print(dbname + "." + tbl_view_name + "." + colname.upper(), file=f4)

    cursor.close()
    cnxn.close()
    f4.close()



def check_obj_name_lengths(path, final_file):
    print ("Checking length of object names")
    obj_names_list = []
    ff = open(path+final_file, "r")
    for line in ff:
        if (line[0:2] == "--"):
            line = next(ff)  # skip line starting with --
        #print("Line = ", line)
        objectname = line[line.find(".")+1:line.find(" ")]
        #print("Object Name = ", objectname)
        objectname = objectname[:objectname.find(".")]
        #print("Object Name 1 = ", objectname)
        #print("-----------------------------")
        obj_names_list.append(objectname)
    uniq_obj_names = set(obj_names_list)
    #print(uniq_obj_names)
    with open(path+final_file[:-4]+"_exceptions.txt", "a") as excp_file:
        excp_file.write("------------------------------\n")
        excp_file.write("Object names greater than 30\n")
        excp_file.write("------------------------------\n")
        for o in uniq_obj_names:
            #print(o, len(o))
            if len(o) > 30:
                excp_file.write(o + " " + str(len(o))+"\n")
    ff.close()   
            


def gen_exception_report(path, final_file, column_master_file):
    er = open(path+final_file[:-4]+"_exceptions.txt", "w")
    ff = open(path+final_file, "r")
    approved_list = []
    exception_list = []
    for line in ff:
        if (line[0:2] == "--"):
            line = next(ff)  # skip line starting with --
            #print("Line after Skip: " + line)
        
        line = line[:line.find(" ")].strip()  # Get the databasename.tablename.columnname only.
        if line in open(path+column_master_file).read():
            print("APPROVED: "+line, file=er)
            approved_list.append(line)
        else:        
            #print("NOT APPROVED: "+line, file=er)
            exception_list.append(line)

    exception_list.sort()
    print ("-"*80, file=er)
    print ("-- Exceptions are below --", file=er)
    print ("-"*80, file=er)
    for line in (exception_list):
        print(line, file=er)

    # Write summary of counts in the exceptions report
    print("-" *80, file=er)
    print("Total Approved  elements = : " + str(len(approved_list)), file=er)
    print("Total Exception elements = : " + str(len(exception_list)), file=er)

    ff.close()
    er.close()  # close exception report (er) file





def main():
    timestarted = time.asctime( time.localtime(time.time()) )
    timestartedseconds = time.monotonic()

    #Step 1: Cleans input file by removing single line, multi line comments, TITLE strings, COMPRESS strings, INSERT statements, keywords like LOCKING ROW, CHECKSUM etc.  Writes <filename>_outfile.txt
    clean_file(path, filename)
    print("Step 1: Clean input file, COMPLETED")
    print("*-" *40)

    #Step 2: Reads output of Step 1 and writes <filename>_final.txt in the format Databasename.Tablename.Colname.  ALTER TABLE DROP statements are marked with commented line (--) Before and After the line.
    with open(path+filename[:-4]+"_outfile.txt") as f:
        full_line = ''
        print("-- Main() --> Reading outfile.txt line by line --")
        for line in f:
            line = line.upper().strip()  # turn each line read to UPPER case and strip
            #print("Line Read: "+line)   # uncomment to see everyline read from line
            
            if (line.find("DECIMAL")>=0): # convert , to : if datatype is DECIMAL.  this is to avoid issue in split the full_line by comma later to get each column and data type separately.
                if (line.count(",") > 1):
                    line = line.replace(",", ":", 1)
                elif(line[-1:] != ","):
                    line = line.replace(",", ":", 1)
                #print("Line after DECIMAL comma replaced with : fix: "+line)

            if (line.find("NUMERIC") >=0):  # to handle NUMERIC(30,10) scenario
                if (line.count(",") > 1) or (line[-1] != ","):
                    line = line.replace(",", ":", 1)
                #print("Line after NUMERIC comma replaced with : fix: "+line)
                
            if (line.find(";") >= 0):
                #print("last line")
                #print (full_line+ " " +line)
                full_line = full_line + " " + line
                #print ("--End of Statement or Single line statement --")
                if (full_line.find("UNIQUE PRIMARY INDEX ") >=0 ):
                    full_line = full_line[:full_line.find("UNIQUE PRIMARY INDEX ")]  # Do not include line after UNIQUE PRIMARY INDEX
                if (full_line.find("PRIMARY INDEX ") >=0 ):
                    full_line = full_line[:full_line.find("PRIMARY INDEX ")]  # Do not include line after PRIMARY INDEX
                full_line = full_line.replace("\t", " ")  # replace any tabs with space
                #print("Full line is = " + full_line)
                
                get_table_cols(full_line+";")
                full_line = ''
            else:
                full_line = full_line.strip() +" "+  line
                #print("---------")
        f.close()
        print("Step 2: Write to final file in format (Database.Table.column) "+filename[:-4]+"_final.txt COMPLETED")
        print("*-" *40)

    #f3.close()  # Close final output file that has Dbname.Tablename.Columnname

    #print("-"*80, file=f3)
    print("-- Objects Touched in this script --", file=f3)  # f3 is <input filename>_final.txt
    #print("-"*80, file=f3)

    obj_touched_set = set(obj_touched_list)  # this will give a unique set of list elements i.e. unique names of objects touched.
    obj_touched_unique_list = list(obj_touched_set) # convert this back to a list
    obj_touched_unique_list.sort()

    #print(obj_touched_unique_list)

    for t in obj_touched_unique_list:
        db_name = t[:t.find(".")]
        obj_name = t[t.find(".")+1:]
        print (db_name.strip() + ", "+obj_name.strip())
        #get_audit_columns(db_name,obj_name)
        print(db_name.strip()+"."+obj_name.strip(), file=f3)
    f3.close()
    print("Step 3: Write Objects touched in the script to final file  COMPLETED")
    print("*-" *40)

    file_audit_columns(obj_touched_unique_list)
    print("Step 4: Column Master table read and filed, COMPLETED")
    print("*-" *40)
    gen_exception_report(path, filename[:-4]+"_final.txt", filename[:-4]+"_column_master.txt" )
    print("Step 5: Create Exception Report, COMPLETED")
    print("*-" *40)

    #get_confidential_list()
    timefinished = time.asctime( time.localtime(time.time()) )
    print("-"*80)
    print("-- SUMMARY --")
    print("*-" *40)
    print("Exception report is saved as = "+ path+filename[:-4]+"_final_exceptions.txt")
    print("Time Started  = " + str(timestarted))
    print("Time Finished = " + str(timefinished))
    print("Time Elapsed (mins) = ", round((time.monotonic() - timestartedseconds)/60,2))

    #get_confidential_list()

    check_obj_name_lengths(path, filename[:-4]+"_final.txt")

# run main program                           
main()


