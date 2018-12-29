import json
import tweepy
from kafka import SimpleProducer, KafkaClient

# json file 'config/twitter-config.json' necessary
with open('config/twitter-config.json') as f:
    config = json.load(f)

#change this to your input topic name
twitter_input_topic = 'twitter_in'

mapping_dict = {'FR': 'Paris',
                'AT': 'Vienna',
                'CH': 'Zurich',
                'IT': 'Rome'}

class StdOutListener(tweepy.StreamListener):
    
    ''' Handles data received from the stream. '''
    
    def __init__(self, api):
        
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True,
                          batch_send_every_t = 2)
 
    def on_status(self, status):
        
        status_dict = {}
        
        try:
            #known error checking
            if status.place == None:
                self.on_error('place_none')
            elif len(status.place.country_code) != 2:
                self.on_error('invalid_cc')

            #get text
            try:
                status_dict['tweet'] = status.extended_tweet['full_text']
            except AttributeError:
                status_dict['tweet'] = status.text

            #get country code
            if status.place.country_code == 'DE':

                lon = int(status.place.bounding_box.coordinates[0][0][0])

                if lon == 13:
                    status_dict['city'] = 'Berlin'
                elif lon == 11:
                    status_dict['city'] = 'Munich'

            else:
                status_dict['city'] = mapping_dict[status.place.country_code]

            #get timestamps
            status_dict['datestring'] = str(status.created_at)

            #get hashtags
            hashtags = []
            for hashtag in status.entities['hashtags']:
                hashtags.append(hashtag['text'])
            status_dict['hashtags'] = hashtags
            
            print('>', end = '')
            self.producer.send_messages(twitter_input_topic, 
                                        str.encode(json.dumps(status_dict, ensure_ascii = False)))
        
        #unknown error
        except Exception as e:
            self.on_error(e)
    
    def on_error(self, status_code):
        print('Error: ' + str(status_code))
        return True # To continue listening
 
    def on_timeout(self):
        return True # To continue listening
    
    
if __name__ == '__main__':
    
    auth = tweepy.OAuthHandler(config['api-key'], config['api-secret-key'])
    auth.set_access_token(config['access-token'], config['access-token-secret'])
    
    api = tweepy.API(auth)
    listener = StdOutListener(api)
 
    stream = tweepy.Stream(auth, listener)
    loc_ls = [2.229335, 48.819298, 2.416376, 48.902579,
              12.358222, 41.777618, 12.632567, 42.016053,
              13.097992, 52.379442, 13.712705, 52.667389,
              11.373648, 48.063202, 11.699651, 48.240582,
              16.192143, 48.114450, 16.551614, 48.326583,
              8.460501, 47.324890,  8.616888, 47.432676]
    stream.filter(locations=loc_ls)