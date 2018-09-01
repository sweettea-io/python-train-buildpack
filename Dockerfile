FROM python:3.6

# Build arg representing the build target uid.
ARG TARGET_UID

# Install buildpack dependencies.
RUN pip install boto3==1.4.7 pyyaml==3.12 git+https://github.com/nicois/redis-py.git@master

# Copy the current directory contents into the container at /app
COPY . /app

# Install project dependencies (if project provides a requirements.txt).
RUN if [ -e /app/requirements.txt ]; then pip install -U pip && pip install -r /app/requirements.txt; fi

# Run the app.
CMD ["python", "app/main_$TARGET_UID.py"]