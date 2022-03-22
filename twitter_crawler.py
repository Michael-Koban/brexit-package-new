import requests
import os
import json
import pandas as pd
import csv , datetime, unicodedata, time, datetime, tweeterid #dateutil.parserm, 


class TwitterCrawler():
    """Summary of class here.

    You must define a TwitterCrawler instance to start crawling Twitter Data.
    Longer class information....

    Attributes:
        likes_spam: A boolean indicating if we like SPAM or not.
        eggs: An integer count of the eggs we have laid.
    """ 
     
    def __init__(self, bearer_token: str):
        """Inits SampleClass with blah."""
        self.bearer_token = bearer_token
        os.environ['TOKEN'] = self.bearer_token
        self.headers = {"Authorization": "Bearer {}".format(self.bearer_token)}

    def auth():
     return os.getenv('TOKEN')

    # def create_headers(self):
    #     """ Creates a header for the Twitter API request,
    #      based on the Bearer Token

    #     Parameters
    #     ----------
    #     sound : str, optional
    #         The sound the animal makes (default is None)

    #     Raises
    #     ------
    #     NotImplementedError
    #         If no sound is set for the animal or passed in as a
    #         parameter.
    #     """

    #     headers = {"Authorization": "Bearer {}".format(self.bearer_token)}
    #     return headers

    def __connect_to_endpoint(self, url: str, params: dict, next_token: str  = None, sleep_time:int = 3, verbose:bool = False):
            params['next_token'] = next_token   #params object received from create_url function

            for i in range(10):

                response = requests.request("GET", url, headers = self.headers, params = params)
                if verbose: print(f"Trail #{i} Response Code: " + str(response.status_code))

                if response.status_code == 200: return response.json()
                
                if response.status_code == 429:
                    if verbose: print(f"Try sleeping for {sleep_time} seconds")
                    time.sleep(sleep_time)

            raise Exception(response.status_code, response.text)

    # def __connect_to_endpoint(self, url: str, params: dict, next_token = None, verbose = True):
    #     """ Connect to Twitter API via a endpoint

    #     Parameters
    #     ----------
    #     sound : str, optional
    #         The sound the animal makes (default is None)

    #     Raises
    #     ------
    #     NotImplementedError
    #         If no sound is set for the animal or passed in as a
    #         parameter.
    #     """
    #     params['next_token'] = next_token   #params object received from create_url function
    #     response = requests.request("GET", url, headers = self.headers, params = params)
    #     if verbose == True:
    #         print("Endpoint Response Code: " + str(response.status_code))
    
    #     if response.status_code == 429:
    #         print(response.text)
    #         print("try sleeping for 2 seconds")
    #         time.sleep(2)
    #         return response.status_code

    #     if response.status_code != 200:
    #         raise Exception(response.status_code, response.text)
    #     return response.json()
    
    def get_url_by_tweet_id(tweet_id: str):
        """ Connect to Twitter API via a endpoint

        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
        return "https://twitter.com/anyuser/status/" + str(tweet_id)

    def search_by_tweet_id(self, tweet_id: str):
        """ search url for idtweet - App rate limit: 
        300 requests per 15-minute window shared among all users of your app
        User rate limit (OAuth 1.0a): 900 requests per 15-minute window per each authenticated user


        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
    
        search_url = "https://api.twitter.com/2/tweets/:id"
        search_url = search_url.replace(":id", tweet_id)
        #change params based on the endpoint you are using
        query_params = {
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        json_response = self.__connect_to_endpoint(url = search_url, params = query_params)
        return json_response

    def search_recent_by_keyword(self, keyword: str, start_date, end_date, max_results = 10):
        """ search url for idtweet - App rate limit: 
        300 requests per 15-minute window shared among all users of your app
        User rate limit (OAuth 1.0a): 900 requests per 15-minute window per each authenticated user
         ONLY FROM LAST WEEK !@#!@#!@#!@#@#!@#!@#!@

        Parameters
        ----------
        sound : str, optional
            The sound the animal makes (default is None)

        Raises
        ------
        NotImplementedError
            If no sound is set for the animal or passed in as a
            parameter.
        """
        search_url = "https://api.twitter.com/2/tweets/search/recent" 

        #change params based on the endpoint you are using
        query_params = {'query': keyword,
                        'start_time': start_date,
                        'end_time': end_date,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}

        json_response = self.__connect_to_endpoint(url= search_url, params = query_params)
        return json_response 
    
###self.__connect_to_endpoint2(self, search_url, query_params, next_token = next_token, verbose = False)
    


    def __return_tweets_of_key_opinion_leader(self, query="", user_name=None,
                                        start_time = "2015-12-7T00:00:00Z",
                                        end_time = "2021-12-26T00:00:00Z",
                                        max_results = 10, evaluate_last_token = False,
                                        limit_amount_of_returned_tweets = 10000000,
                                       verbose_10 = False, dir_name = "key_opinion_leaders_tweets_tables_beta"
):
  
        search_url = "https://api.twitter.com/2/tweets/search/all" #endpoint use to collect data from

        import os.path
        try:
            os.mkdir(dir_name)
            print("creating directory", dir_name, "to insert all the tables of all the key opinion leaders")
        except:
            print("The dir", dir_name ,"already exist")

        if user_name is not None:
            query = str(query) + " from:" + str(user_name)
            try:
                display_start_time = datetime.strptime(start_time.split("T")[0], "%Y-%m-%d").strftime("%d-%m-%Y")
                display_end_time = datetime.strptime(end_time.split("T")[0], "%Y-%m-%d").strftime("%d-%m-%Y")
            except:
                display_start_time = start_time
                display_end_time = end_time
            print("Bringing all the tweets of the user:", user_name, "from:", display_start_time, "to", display_end_time)
            print()

        ##### the log dir
        import os.path
        #dir_name = "key_opinion_leaders_tweets_tables_beta"
        dir_name_beta = "key_opinion_leaders_tweets_tables_beta"
        dir_log_name = os.path.join(dir_name_beta, "log_key_opinion_leaders") 

        # try:
        #     os.mkdir(dir_name)
        #     print("creating directory", dir_name, "to insert all the tables of all the key opinion leaders")
        # except:
        #     print("The dir", dir_name ,"already exist")
        try:
            os.mkdir(dir_log_name)
            print("creating directory", dir_log_name, "to insert all the logs of the key opinion leaders")
        except:
            print("The dir", dir_log_name ,"already exist")
        ########################
        path_for_log_dir_of_certain_user = os.path.join(dir_log_name, user_name)
        try:
            os.mkdir(path_for_log_dir_of_certain_user)
            print("creating directory", path_for_log_dir_of_certain_user,"in the dir",dir_log_name, "to insert all the logs of the key opinion leader", user_name)
        except:
            print("The dir", path_for_log_dir_of_certain_user ,"already exist")
            
        path_for_dir_retriving_tweets_streem = os.path.join(path_for_log_dir_of_certain_user, 'retriving_tweets_streem.txt')
        with open(path_for_dir_retriving_tweets_streem, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time + '\n\n')
            
        ########### If the token file exist already, then take the last token available, else start from token 1  ############ 
        tokens_location = os.path.join(dir_log_name, user_name, "tokens.txt") 

        if (evaluate_last_token == True and os.path.isfile(tokens_location) == True):
            a_file = open(tokens_location, "r")
            lines = a_file.readlines()
            last_lines = lines[-2]
            next_token = last_lines[0:-1]
            a_file.close()    
        else:
            next_token = None
            
        ################ Add a time stamp ########################################
        path_for_dir_tokens = os.path.join(path_for_log_dir_of_certain_user, 'tokens.txt')
        with open(path_for_dir_tokens, 'a') as f:
            from datetime import datetime
            now = datetime.now()
            current_time = now.strftime("%H:%M:%S (Date: %d.%m.%y)")
            current_time = "Current Time: " + current_time + "   *****************************************   "
            f.write(current_time+ '\n\n')
            
        ##########################################################################################

        continue_searching = True
        json_response_list = []
        next_tokens = []
        num_of_returned_tweets = 0
        counter_loops = 0
        
        while continue_searching == True and num_of_returned_tweets < limit_amount_of_returned_tweets:
            counter_loops +=1
            if counter_loops > 1:
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                print("token to insert:",next_token)
            #if the returned amount of tweets is getting close to the limit number, we need to alter the max_result,
            #so we won't get tweets beyond what we asked
            
            if (limit_amount_of_returned_tweets - num_of_returned_tweets) < max_results:
                max_results = limit_amount_of_returned_tweets - num_of_returned_tweets
                if max_results < 10:
                    max_results = 10
            else :
                max_results = max_results

            #change params based on the endpoint you are using
            query_params = {'query': query,
                        'start_time': start_time,
                        'end_time': end_time,
                        'max_results': max_results,
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id,entities.mentions.username,referenced_tweets.id',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {next_token}}
            
            json_response = self.__connect_to_endpoint(url = search_url,params= query_params, next_token = next_token)
            
            json_response_list.append(json_response) #the first json_response itme
            num_of_returned_tweets += json_response["meta"]["result_count"]

            ##### making a dataframe out of the json response:
            try:
                a = pd.json_normalize(json_response["data"])
                b = pd.json_normalize(json_response["includes"], ["users"]).add_prefix("users.")
                
                a.conversation_id = a.conversation_id.astype("string")
                a.id = a.id.astype("string")
                a["id_new"] = "id: " + a["id"].astype("string")
                a["conv_id_new"] = "conv_id: " + a["conversation_id"].astype("string")
            #c = pd.json_normalize(json_response["includes"]["places"]).add_prefix("places.")
                df_tweets_i = pd.merge(a, b, left_on="author_id", right_on="users.id")
                list_of_cols_to_add = ['author_id', 'conversation_id', "conv_id_new", "id", "id_new",
                                    'created_at','entities.mentions',
                            'public_metrics.like_count', 'public_metrics.quote_count',
                        'public_metrics.reply_count', 'public_metrics.retweet_count','referenced_tweets', 'text',
                            'users.created_at', 'users.description','users.id', 'users.name',
                        'users.public_metrics.followers_count', 'users.public_metrics.following_count',
                        'users.public_metrics.listed_count', 'users.public_metrics.tweet_count',
                        'users.username', 'users.verified']

                list_cols_to_drop = [x for x in df_tweets_i.columns if x not in list_of_cols_to_add]

                ##droping labels we don't need
                df_tweets_i = df_tweets_i.drop(labels=list_cols_to_drop, axis = 1, errors = "ignore")

                for col in list_of_cols_to_add:
                    if col not in df_tweets_i.columns:
                        df_tweets_i[col] = "NA"

                #sort columns by alphabetic order
                col_list_df_tweets_i = df_tweets_i.columns.tolist()
                col_list_df_tweets_i.sort()
                df_tweets_i = df_tweets_i.reindex(columns=col_list_df_tweets_i)
                
                name = user_name + ".csv"
                path_for_table = os.path.join(dir_name_beta, name)
                if os.path.isfile(path_for_table) == False: #if this is the first table of tweets
                    df_tweets_i.to_csv(path_for_table, index=True)
                else:
                    df_tweets_i.to_csv(path_for_table, mode='a', index=True, header=False)
            except:
                print("no data / include in the json")
        
            with open(path_for_dir_retriving_tweets_streem, 'a') as f:
                print_stat = str(counter_loops) + " -> Got from twitter " + str(json_response["meta"]["result_count"]) + " tweets, and there are more tweets of that user to get, I am bringing more tweets!"
                f.write(print_stat+'\n')
                print_total = "Total amount of tweets: " + str(num_of_returned_tweets)
                f.write(print_total+ '\n\n')

            if "next_token" in json_response["meta"]:
                if (verbose_10 == True and counter_loops % 20 == 1):
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                elif verbose_10 == False:
                    print(counter_loops, "Got from twitter", json_response["meta"]["result_count"], "tweets, and there are more tweets of that user to get, I am bringing more tweets!\n")
                next_token = json_response["meta"]["next_token"]
                query_params["next_token"] = next_token
                next_tokens.append(next_token)
                #ids_token_print = "next token = " + next_token + "newest id: " + json_response["meta"]["newest_id"] + " | oldest id: " + json_response["meta"]["oldest_id"]
                ids_token_print = next_token
                with open(path_for_dir_tokens, 'a') as f:
                    f.write(ids_token_print + '\n\n')
            else:
                print("no more tweets from this user")
                continue_searching = False
                print("Total amount of collected tweets = ", num_of_returned_tweets)
            
            if num_of_returned_tweets >=limit_amount_of_returned_tweets:
                print("oooops, There may be more tweets to return, but you asked to limit the amount of returned tweets")
                print("infact you got", num_of_returned_tweets, "returned tweets and limited the function to get", limit_amount_of_returned_tweets, "tweets")
        
        #In what case we suspect that there may be more tweets that we didn't get? -->
        #When the number of tweets we asked to get is equal to the number of tweets we got back
        
        ### save all thr json responses in json file:
        path_for_dir_all_json_responses = os.path.join(path_for_log_dir_of_certain_user, 'all_json_responses.json')
        with open(path_for_dir_all_json_responses, 'w') as outfile:
            json.dump(json_response_list, outfile)
        return json_response_list, num_of_returned_tweets, next_tokens

 #The following cose check wheter the user_name is a list, if not it turnes it into a list
      
    def return_tweets_of_key_opinion_leaders(self, query="", dir_name="tweets", user_names =None, \
        start_time = "2015-12-7T00:00:00Z", end_time = "2021-12-26T00:00:00Z",
        max_results = 10, evaluate_last_token = False, \
            limit_amount_of_returned_tweets = 10000000, verbose_10 = False):

            if type(user_names) != list:
                user_names = [user_names]
            
            #users_json_response_lists = []
            names_evaluated = []
            names_didnt_evaluated = []
            next_tokens_users= [] #this will include a list where eachelement is a list containing all the tokens off the specific user
            for name in user_names:
                print("Bringing tweets of", name)
                query = ""
                user_name = name
                try:

                    json_response_list, num_of_returned_tweets,next_tokens = self.__return_tweets_of_key_opinion_leader(query=query, user_name=user_name,
                                                            start_time = start_time,
                                                            end_time = end_time,
                                                            max_results = max_results,
                                                            limit_amount_of_returned_tweets = limit_amount_of_returned_tweets,
                                                                                                                verbose_10 = True)
                    print(num_of_returned_tweets)
                    if num_of_returned_tweets > 0:
                        names_evaluated.append(name)
                        next_tokens_users.append(next_tokens)
                

                    else:
                        names_didnt_evaluated.append(name)
                        print("The user:", name, "had", num_of_returned_tweets, "tweets!!")

                    print("---------------------------------------------------------------")
                except:
                    print("There was a problem with the key opinion leader:", name)
                    names_didnt_evaluated.append(name)
                    print("*************************************************************************************")

    import time, datetime,json, numpy as np
    from inputimeout import inputimeout, TimeoutOccurred


    def create_url_tweet_ids(self, search_url, tweet_ids_list, verbose = False):
    
        #search_url = "https://api.twitter.com/2/tweets/search/recent" #Change to the endpoint you want to collect data from
        search_url = search_url.replace("X", ','.join(tweet_ids_list))
        if verbose: print(search_url)
        #change params based on the endpoint you are using
        query_params = {
                        'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                        'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                        'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                        'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                        'next_token': {}}
        return (search_url, query_params)



    #   sleep_time = 15*60 ## 15 minutes in sec
    #   tweet_ids = all_harvard_data["ID"].to_numpy().astype(str)

    from os import path


    def get_tweets_by_tweet_ids(self , tweet_ids, #TypeError: 'type' object is not subscriptable 
     json_tweets_output_folder : str,
     tweets_per_api_request : int = 100,
     api_error_sleep_secs : int = 15*60,
     verbose = False ):

        os.makedirs(json_tweets_output_folder, exist_ok = True) 

        count_proccesed_tweets_file = os.path.join(json_tweets_output_folder, 'count_proccesed_tweets.txt')
        count_requests_sent_file = os.path.join(json_tweets_output_folder,'count_requests_sent.txt')
        json_output_file_basename = os.path.join(json_tweets_output_folder,'twitter_data_')

        if not os.path.exists(count_requests_sent_file):
            with open(count_requests_sent_file,'w') as f:
                f.write('%d' % 0)
        
        if not os.path.exists(count_proccesed_tweets_file):
            with open(count_proccesed_tweets_file,'w') as f:
                f.write('%d' % 0)
        
        count_proccesed_tweets = int( open(count_proccesed_tweets_file,'r').read())   

        count_requests_sent = int( open(count_requests_sent_file,'r').read())

        while(count_proccesed_tweets < len(tweet_ids)):
            
            if verbose: 
                print(f'~~~~~ Already sent {count_requests_sent} requests')
                print(f'~~~~~ Already proccesed {count_proccesed_tweets} tweets')
                print(f'~~~~~ Currently proccessing {tweets_per_api_request} tweets')

            
            ## FIXME - last batch size cant be more than what's left 

            tweet_ids_batch = tweet_ids[count_proccesed_tweets:(count_proccesed_tweets+ tweets_per_api_request )// (len(tweet_ids)-1)]
            search_url, query_params = self.create_url_tweet_ids("https://api.twitter.com/2/tweets/?ids=X" , tweet_ids_batch)
            # headers = create_headers(os.environ['TOKEN'])

            try:
                json_response = self.__connect_to_endpoint( url = search_url, params= query_params)
            except Exception as e: 
                print(f'Twitter API error: {e} \n Sleeping 15 min from {datetime.datetime.now()}')
                time.sleep(api_error_sleep_secs)

                    
            count_proccesed_tweets += tweets_per_api_request
            count_requests_sent += 1

            
            with open(f'{json_output_file_basename}{count_requests_sent}.json', "w") as twitter_data_file:
                json.dump(json_response, twitter_data_file, indent=4, sort_keys=True) 
                
            with open(count_proccesed_tweets_file, "w") as f:
                f.write('%d' % count_proccesed_tweets)
            
            with open(count_requests_sent_file, "w") as f:
                f.write('%d' % count_requests_sent) 
            
            # if verbose: print(f'~~~~~ Batch done, moving forward (sleep 2 min for debug)')
            # time.sleep(5)  

