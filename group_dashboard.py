from flask import Flask,jsonify,request
from flask import render_template
import ast

app = Flask(__name__, template_folder='templates')
timestamp_labels = []
twitter_values = []
bitcoin_values = []
predicted_values = []
  
@app.route("/")
def get_chart_page():
    global twitter_values, bitcoin_values, predicted_values, timestamp_labels
    return render_template('bitcoin_viz.html', twitter_values= twitter_values, bitcoin_values = bitcoin_values, predicted_values = predicted_values, timestamp_labels = timestamp_labels)

@app.route('/refreshData')
def refresh_graph_data():
    global twitter_values, bitcoin_values, timestamp_labels, predicted_values
    print("timestamp labels now: " + str(timestamp_labels))
    print("twitter data now: " + str(twitter_values))
    print("bitcoin data now: " + str(bitcoin_values))
    print("predicted data now: " + str(predicted_values))
    return jsonify(sLabel=timestamp_labels, sTData= twitter_values, sBData= bitcoin_values, sPData= predicted_values)

@app.route('/updateData', methods=['POST'])
def update_data():
    global timestamp_labels, twitter_values, bitcoin_values, predicted_values
    #if not request.form or 'twitter_data' or 'bitcoin_data' not in request.form:
    #    return "error",400
    timestamp_labels = ast.literal_eval(request.form['timestamp_label'])
    twitter_values = ast.literal_eval(request.form['twitter_data'])
    bitcoin_values = ast.literal_eval(request.form['bitcoin_data'])
    predicted_values = ast.literal_eval(request.form['predicted_data'])
    print("timestamp labels received: " + str(timestamp_labels))
    print("twitter data received: " + str(twitter_values))
    print("bitcoin data received: " + str(bitcoin_values))
    print("predicted data received: " + str(predicted_values))
    return "success",201

if __name__ == "__main__":
    app.run(host='localhost', port=5001, debug = True)
    
    #src = 'static/bitcoin_viz_script.min.js'