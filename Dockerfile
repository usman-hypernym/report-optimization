FROM python:3.8
ENV PYTHONUNBUFFERED 1
ENV PYTHONDONTWRITEBYTECODE 1
 
# Set the working directory in the container
WORKDIR /code
 
 
# Install FFmpeg
RUN apt-get update \
&& apt-get install -f \
&& rm -rf /var/lib/apt/lists/*
 
 
COPY requirements.txt .
 
# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt
 
COPY . .
 
