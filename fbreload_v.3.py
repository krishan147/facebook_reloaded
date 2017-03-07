import time
import json
import urllib
import datetime
import logging
import os
import pyodbc
import timeit
import sys
import gspread
from oauth2client.service_account import ServiceAccountCredentials
start = timeit.default_timer()
str_logging_time = str(datetime.datetime.now())
time_start = datetime.datetime.now()

### WRITING TO GOOGLE SHEET

scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('ENTER PROJECT NAME HERE', scope)
gc = gspread.authorize(credentials)
wks = gc.open("BRPMEN_Bingboard").sheet1
wks.update_acell('A20', 'Running...')
wks.update_acell('B21', 'Will populate when process is finished')
wks.update_acell('B22', 'Will populate when process is finished')
wks.update_acell('B23', 'Will populate when process is finished')
wks.update_acell('B24', 'Will populate when process is finished')
wks.update_acell('B25', 'Will populate when process is finished')
wks.update_acell('B26', 'Will populate when process is finished')

wks.update_acell('D20', '')
wks.update_acell('E20', '')

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

original_date = datetime.datetime.now() #TIME NOW

facebook_count = "100" # facebook max 100 

### GET TOTAL NUMBER OF FACEBOOK ON THE SYSTEM BEFORE THE PROCESS BEGAN

amvbbdo_brpmen = 'DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD'
table_name_total = "BRPMEN_Posts"
cnxn = pyodbc.connect(amvbbdo_brpmen)
cursor = cnxn.cursor()
cursor.execute("SELECT fb_post_id FROM "+table_name_total+ " ORDER BY fb_post_id DESC")
rows = cursor.fetchall()
cnxn.close()

num_of_posts_before_running_script = 0
for item in rows:
    if "None" in str(item):
        pass
    else:
        num_of_posts_before_running_script = num_of_posts_before_running_script + 1

str_num_of_posts_before_running_script = str(num_of_posts_before_running_script)

### CONNECT TO DB

database_details = 'DRIVER={SQL Server};SERVER=ENTER SERVER NAME;DATABASE=ENTER DATABASE NAME;UID=ENTER USER ID;PWD=ENTER PASSWORD'
table_name = 'BRPMEN_POSTS'
address_book_table = 'BRPMEN_AddressBookSocialProfiles'

### COPY ALL FACEBOOK IDS FROM THE DATABASE TO AVOID ADDING DUPLICATES

cnxn = pyodbc.connect(database_details)
cursor = cnxn.cursor()
cursor.execute("select fb_post_id from "+table_name+" WHERE fb_post_id IS NOT NULL")
fetch = cursor.fetchall()

with open("facebook_database_of_facebook_ids.txt", "a") as database_of_facebook_ids_append:
    for item in fetch:
        database_of_facebook_ids_append.write(item[0])

### WORK THROUGH ALL USER IDs IN THE DATABASE
        
cnxn = pyodbc.connect(database_details)
cursor = cnxn.cursor()
cursor.execute("SELECT matchkey, country, location, venue_name, uniqueid from "+address_book_table+" WHERE platform LIKE 'fb' ORDER BY last_checked asc") # ORDER BY facebook_id ASC
#cursor.execute("SELECT matchkey, country, location, venue_name, uniqueid from "+address_book_table+" WHERE platform LIKE 'fb' AND location LIKE 'New York City' ORDER BY last_checked ASC") # ORDER BY facebook_id DESC"
rows = cursor.fetchall()
row_string = str(rows)
num_of_new_posts = 0

### FACEBOOK LOOP 1

for item_2 in rows: # ANALYSE EACH HANDLE ONE AT A TIME

    if item_2: # some items in the db are empty - this line will disregard empty elements
        if is_number(item_2[0]):
            facebook = item_2[0] #FACEBOOK ID
            time_per_handle1 = time.time()
            if facebook:
                country = item_2[1] #COUNTRY
                city = item_2[2] #CITY
                name = item_2[3] #VENUE NAME
                users_uniqueid = item_2[4] #required for updating last checked date
                BRPMEN_AddressBookSocialProfiles_uniqueId = item_2[4] #required for updating last checked date          
                #print facebook, name, country, city

                ms_sql_time = datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')
                original_date = datetime.datetime.now()
                stringed_date = str(original_date)
                replacer_date = stringed_date.split(".",1)[0]
                date_comparison_now = time.mktime(datetime.datetime.strptime(replacer_date, "%Y-%m-%d %H:%M:%S").timetuple())
                date_comparison_now_s = str(date_comparison_now)

                year_timestamp = 31591981
                day_timestamp = 41210
                time_then = date_comparison_now - day_timestamp 

                time.sleep(2)
                access_token = 'ENTER ACCESS TOKEN'
                url = "https://graph.facebook.com/v2.5/"+facebook+"?fields=posts.limit("+facebook_count+").until("+date_comparison_now_s+").since("+str(time_then)+")%7Bmessage%2Ccreated_time%7D&access_token=" + access_token #CALLING FACEBOOK API
                opener = urllib.urlopen(url)
                sleeper = 1
                                                                                                                
                for item in opener:
                    try:
                        time.sleep(sleeper)
                        parsed_one = json.loads(item)
                        if ('posts' in parsed_one) and ('data' in parsed_one["posts"]):
                            fbdata = parsed_one["posts"]["data"]
                            time.sleep(1)
                            count_of_added_posts = 0
                            fb_post_id_already_exists = 0
                            for fbpost in fbdata:
                                if 'message' in fbpost:
                                    fb_post_id = fbpost["id"] #FACEBOOK POST ID
                                    fb_post_id_s = str(fb_post_id)
                                    f = open('facebook_database_of_facebook_ids.txt', 'r')
                                    lines = f.read()
                                    

                                    if fb_post_id_s not in lines:
                                        count_of_added_posts = count_of_added_posts+1                                   
                                        with open("facebook_database_of_facebook_ids.txt", "a") as database_of_facebook_ids_append: #add the status ID to the database of statuses
                                            database_of_facebook_ids_append.write(fb_post_id_s)

                                        if fbpost:

                                            
                                            time.sleep(sleeper)
                                            fbpost_text2 = fbpost["message"] #MESSAGE                       
                                            created = fbpost['created_time'] 
                                            clean_created = created.replace("+0000","") #CREATED DATE
                                            fb_id_number_string = str(facebook)
                                            fb_id_number_final = fb_id_number_string.replace("\n","") #FACEBOOK ID
                                            fbpost_text2_cut = fbpost_text2[0:2999]
                                            time.sleep(1)
                                            fb_post_id_string = str(fb_post_id)

                                            encode_post = fbpost_text2_cut.encode('ascii', 'ignore').decode('ascii')
                                            post_nogap = encode_post.replace("\n"," ")
                                            post_truncated = post_nogap[:400]

                                            fb_url = "www.facebook.com/"+fb_post_id

                                            #print name, city, fb_post_id, fbpost_text2_cut
                                            time.sleep(5)

                                            print "Adding a new facebook post. Post no. " + str(count_of_added_posts)
                                            print fb_post_id
                                            cnxn = pyodbc.connect(database_details)
                                            cursor = cnxn.cursor()
                                            cursor.execute("insert into BRPMEN_POSTS (facebook_id,fb_post_id,location,venue_name,post,date_posted,country,date_added,fb_url) values (?,?,?,?,?,?,?,?,?)",fb_id_number_final,fb_post_id,city,name,post_truncated,clean_created,country,original_date,fb_url)
                                            cnxn.commit()
                                            cnxn.close()
                                            print "Post successfully added"
                                            num_of_new_posts = num_of_new_posts + 1
                                            
                                    if fb_post_id_s in lines:
                                        fb_post_id_already_exists = 1
                                        print "fb post id already exists"
                                        break
                                                                                 
### FACEBOOK SUBSEQUENT LOOPS
                                    
                            if not fb_post_id_already_exists:
                                if ('paging' in parsed_one["posts"]) and ('next' in parsed_one["posts"]["paging"]):
                                    while 'next' in parsed_one["posts"]["paging"]:
                                        link_acquire_2 = parsed_one["posts"]["paging"]["next"]
                                        opener2 = urllib.urlopen(link_acquire_2)

                                        for item2 in opener2:
                                            time.sleep(2)
                                            parsed_one = json.loads(item2) # overwriting the old json with the new one
                                            message_3 = parsed_one["data"]

                                            for item222 in message_3:
                                                if ('message' in item222) and ('id' in item222) and ('name' in item222):
                                                    fb_post_id2 = item222["id"] #FACEBOOK POST ID
                                                    name2 = item222["name"]

                                                    fb_post_id2_s = str(fb_post_id2)

                                                    if fb_post_id2_s not in lines:
                                                        with open("facebook_database_of_facebook_ids.txt", "a") as database_of_facebook_ids_append: #add the status ID to the database of statuses
                                                            database_of_facebook_ids_append.write(fb_post_id2_s)
                                                        
                                                        created_11 = item222['created_time']
                                                        clean_created2 = created_11.replace("+0000","") #CREATED DATE
                                                        time.sleep(1)
                                                        message_2 = item222['message'] #MESSAGE 
                                                        fb_post_id2 = item222["id"] #FACEBOOK POST ID                                             
                                                         
                                                        fb_post_id2_string = str(fb_post_id2)
                                                        fbpost_text3_cut = message_2[0:2999]

                                                        print city, fb_post_id2, fbpost_text3_cut
                                                        time.sleep(1)

                                                        encode_post = fbpost_text3_cut.encode('ascii', 'ignore').decode('ascii')
                                                        post_nogap = fbpost_text2_cut.replace("\n"," ")
                                                        post_truncated_two = post_nogap[:400]

                                                        print "Adding a new facebook post"
                                                        print fb_post_id
                                                        cnxn = pyodbc.connect(database_details)
                                                        cursor = cnxn.cursor()
                                                        print ("insert into BRPMEN_POSTS (facebook_id,fb_post_id,location,venue_name,post,date_posted,country,date_added) values (?,?,?,?,?,?,?,?)",fb_id_number_final,fb_post_id2,city,name2,post_truncated_two,clean_created2,country,original_date)
                                                        cursor.execute("insert into BRPMEN_POSTS (facebook_id,fb_post_id,location,venue_name,post,date_posted,country,date_added) values (?,?,?,?,?,?,?,?)",fb_id_number_final,fb_post_id2,city,name2,post_truncated_two,clean_created2,country,original_date)
                                                        cnxn.commit()
                                                        cnxn.close()
                                                        print "Post successfully added"
                                                        num_of_new_posts = num_of_new_posts + 1
                                                    if fb_post_id2_s in lines:
                                                        print "fb post id already exists"
                                                        break
                                        if 'posts' not in parsed_one:
                                            break

                                    f.close()
                                                                                                             
### SET LAST CHECKED DATE IN SQL ADDRESS BOOK
                                    
                        cnxn = pyodbc.connect(database_details)
                        cursor = cnxn.cursor()
                        #print ms_sql_time, BRPMEN_AddressBookSocialProfiles_uniqueId
                        cursor.execute("UPDATE BRPMEN_AddressBookSocialProfiles SET last_checked='%s' WHERE uniqueid='%s'" % (ms_sql_time, BRPMEN_AddressBookSocialProfiles_uniqueId))                                                      
                        cnxn.commit()
                        cnxn.close()
                        print "Successfully added last checked date"

### CHECK HOW LONG IT TAKES TO RUN ONE HANDLE. IF NECESSARY PAUSE TO AVOID TWITTER THROTTLING
                        
                        time_per_handle2 = time.time()
                        calculate_run_time = time_per_handle2-time_per_handle1
                        print "time per 1 handle: " + str(calculate_run_time) + " for user ID " + facebook

### IF AN ERROR IS FOUND, IT'S LOGGED HERE                        
                                                                                                                            
                    except Exception, e:
                        with open ('facebook_log.txt', 'w') as error_catch:
                            error_catch.write("Script finished but there was an error: " + str(e) + "\n")

                            str_error = str(e)
                            wks = gc.open("BRPMEN_Bingboard").sheet1
                            wks.update_acell('D20', 'There was an error:')
                            wks.update_acell('E20', str_error)
                
                            print "There was an error. Refer to facebook_log.txt for error message."
                        error_catch.close()
                        #sys.exit()

os.remove("facebook_database_of_facebook_ids.txt")

### TIMER STOPS

stop = timeit.default_timer()
run_time = stop - start
m, s = divmod(run_time, 60)
h, m = divmod(m, 60)
hms_run_time = "%dh %02dm %02ds" % (h, m, s)
total_num_of_posts = num_of_posts_before_running_script + num_of_new_posts
str_total_num_of_posts = str(total_num_of_posts)
str_num_of_new_posts = str(num_of_new_posts)

str_time_end = str(datetime.datetime.now())

### TOP LINE DETAILS ARE LOGGED HERE

with open('facebook_log.txt', 'w') as success_message:
    success_message.write("Script finished successfully!" + "\n")
    success_message.write("Script started at " + str_logging_time + "\n")
    success_message.write("Script stopped at " + str_time_end + "\n")
    success_message.write("This script took " + hms_run_time + " to run" + "\n")
    success_message.write("The number of posts on the system before the script ran was " + str_num_of_posts_before_running_script + "\n")
    success_message.write("The number of posts on the system after the script has ran is " + str_total_num_of_posts + "\n")
    success_message.write(str_num_of_new_posts + " new posts were added")
    print "Script finished. Refer to facebook_log.txt for top line stats"
success_message.close()

scope = ['https://spreadsheets.google.com/feeds']
wks.update_acell('A20', 'Script finished!')
wks.update_acell('B21', str_logging_time)
wks.update_acell('B22', str_time_end)
wks.update_acell('B23', hms_run_time)
wks.update_acell('B24', str_num_of_posts_before_running_script)
wks.update_acell('B25', str_total_num_of_posts)
wks.update_acell('B26', str_num_of_new_posts)
