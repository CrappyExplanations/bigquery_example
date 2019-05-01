require 'net/http'
require 'uri'
require 'json'

uri = URI.parse("http://localhost:8080/bigquery_example")

header = {'Content-Type': 'application/json'}
data = {year: "2017"}

# Create the HTTP objects
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, header)
request.body = data.to_json

# Send the request
response = http.request(request)
puts "Code: #{response.code}"
puts "Headers"
response.each_header do |header, values|
	puts "\t#{header}: #{values.inspect}"
end
puts "Body: #{response.body}"

uri = URI.parse("http://localhost:8080/bigquery_example_fetch")

http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, header)
request.body = response.body

# Send the request
response = http.request(request)

puts "Code: #{response.code}"
puts "Headers"
response.each_header do |header, values|
	puts "\t#{header}: #{values.inspect}"
end
puts "Body: #{response.body}"
