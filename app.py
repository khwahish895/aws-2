from flask import Flask, request, render_template
import boto3

app = Flask(__name__)
s3 = boto3.client('s3')
BUCKET = 'video-upload-bucket-2025'

@app.route('/')
def index():
    return render_template("upload.html")

@app.route('/upload', methods=['POST'])
def upload_video():
    video = request.files['video']
    s3.upload_fileobj(video, BUCKET, video.filename)
    return f"Uploaded {video.filename} to S3."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
