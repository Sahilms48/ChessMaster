import sys
import os
import time

def print_status(step, status, message=""):
    symbol = "✅" if status else "❌"
    print(f"{symbol} {step}")
    if message:
        print(f"   Error: {message}")

def check_python_dependencies():
    required = ['pyspark', 'kafka', 'chess', 'streamlit', 'plotly', 'pyarrow', 'zstandard']
    missing = []
    for pkg in required:
        try:
            if pkg == 'kafka':
                import kafka
            elif pkg == 'chess':
                import chess
            else:
                __import__(pkg)
        except ImportError:
            missing.append(pkg)
    
    if missing:
        print_status("Python Dependencies", False, f"Missing: {', '.join(missing)}")
        return False
    print_status("Python Dependencies", True)
    return True

def check_java():
    # Basic check for JAVA_HOME or java command
    java_home = os.environ.get('JAVA_HOME')
    if not java_home:
        # Try checking if java is in path
        import subprocess
        try:
            subprocess.check_output("java -version", shell=True, stderr=subprocess.STDOUT)
            print_status("Java Runtime", True, "Found in PATH (JAVA_HOME not set)")
            return True
        except:
            print_status("Java Runtime", False, "JAVA_HOME not set and java not in PATH")
            return False
    print_status("Java Runtime", True, f"JAVA_HOME={java_home}")
    return True

def check_kafka():
    from kafka import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable
    try:
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092")
        admin.list_topics()
        admin.close()
        print_status("Kafka Connection", True)
        return True
    except NoBrokersAvailable:
        print_status("Kafka Connection", False, "No brokers available at localhost:9092")
        return False
    except Exception as e:
        # Often fails if kafka-python is installed but no broker running
        print_status("Kafka Connection", False, str(e))
        return False

def check_spark():
    from pyspark.sql import SparkSession
    try:
        spark = SparkSession.builder \
            .appName("SetupVerification") \
            .master("local[*]") \
            .getOrCreate()
        
        # Simple dataframe op
        data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
        df = spark.createDataFrame(data, ["language", "users"])
        count = df.count()
        
        spark.stop()
        print_status("PySpark Session", True, f"Test DataFrame count: {count}")
        return True
    except Exception as e:
        print_status("PySpark Session", False, str(e))
        return False

if __name__ == "__main__":
    print("Starting Environment Verification...\n")
    
    checks = [
        check_java(),
        check_python_dependencies(),
        check_spark(),
        check_kafka()
    ]
    
    if all(checks):
        print("\n✅ All checks passed! Environment is ready.")
        sys.exit(0)
    else:
        print("\n❌ Some checks failed. Please verify your environment.")
        sys.exit(1)

