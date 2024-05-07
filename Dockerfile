FROM python:3.8

# Set the working directory inside the container
WORKDIR /app

# Copy the local code into the container at the working directory
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which the application will run
EXPOSE 5000

# Command to run the application
CMD ["python", "app.py"]
