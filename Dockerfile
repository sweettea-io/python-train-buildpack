FROM tensorci/train-augment-parent

RUN pip install git+git://github.com/nicois/redis-py.git@df6395284674e1acacc68630f86d37dd4f03b5f6

# Copy the current directory contents into the container at /app
COPY . /app

# upgrade pip and install required python packages (if requirements.txt is a thing)
RUN if [ -e /app/requirements.txt ]; then pip install -U pip && pip install -r /app/requirements.txt; fi