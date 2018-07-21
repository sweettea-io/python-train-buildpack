FROM python:3.6-alpine

# Build arg representing the build target uid.
ARG TARGET_UID

RUN pip install requests==2.18.4 boto3==1.4.7 pyyaml==3.12 SQLAlchemy==1.1.15 psycopg2==2.7.3
RUN pip install git+git://github.com/nicois/redis-py.git@master

# Copy the current directory contents into the container at /app
COPY . /app

# upgrade pip and install required python packages (if requirements.txt is a thing)
RUN if [ -e /app/requirements.txt ]; then pip install -U pip && pip install -r /app/requirements.txt; fi

# Run the app.
CMD ["python", "app/main_$TARGET_UID.py"]