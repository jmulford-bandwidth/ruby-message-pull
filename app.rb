require "rubygems"
require "ruby-bandwidth"
require "csv"
require "date"

START_DATE = ENV['START_DATE'] #Format "yyyy-mm-dd HH:MM:SS.SSS"
END_DATE = ENV['END_DATE'] #Format "yyyy-mm-dd HH:MM:SS.SSS"
BANDWIDTH_USER_ID = ENV['BANDWIDTH_USER_ID']
BANDWIDTH_API_TOKEN = ENV['BANDWIDTH_API_TOKEN']
BANDWIDTH_API_SECRET = ENV['BANDWIDTH_API_SECRET']


def get_messages(client, start_date, end_date)
  begin
    for x in 0..1000
      messages = Bandwidth::Message.list(client, {:fromDateTime => start_date, :toDateTime => end_date, :size => 2})
    end
  rescue Bandwidth::Errors::GenericError => e
    #Wait 5 seconds, then try again
    sleep(5)
    get_messages(client, start_date, end_date)
  end
  return messages
end

#bw_datetime is a string in the format "yyyy-mm-dd hh:mm:ss.sss"
#ruby_datetime is a datetime object
def convert_bw_datetime_to_ruby_datetime(dt)
  return DateTime.parse(dt)
end

def convert_ruby_datetime_to_bw_datetime(dt)
  return "%s-%s-%s %s:%s:%s" % [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second]
end

CSV.open("data_test_2.csv", "w") do |csv|
  client = Bandwidth::Client.new(:user_id => BANDWIDTH_USER_ID, :api_token => BANDWIDTH_API_TOKEN, :api_secret => BANDWIDTH_API_SECRET)

  messages = get_messages(client, START_DATE, END_DATE)
  csv << messages.first.keys

  start_date = convert_bw_datetime_to_ruby_datetime(START_DATE)
  end_date = convert_bw_datetime_to_ruby_datetime(END_DATE)
  while start_date < end_date
    messages = get_messages(client, convert_ruby_datetime_to_bw_datetime(start_date), convert_ruby_datetime_to_bw_datetime(end_date))
    if messages.length > 0
      messages.each do |x|
        csv << x.values
      end
      end_date = DateTime.parse(messages[-1][:time])
    else
      break
    end
  end
end
