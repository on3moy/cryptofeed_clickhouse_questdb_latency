import subprocess
import time

SCRIPTS = {
    "Trades": "/cryptofeed/src/script/cryptofeed_1_trades.py",
    "Orderbooks": "/cryptofeed/src/script/cryptofeed_2_orderbooks.py"
}

def start_process(name, script):
    print(f"Starting {name} process...")
    return subprocess.Popen(["python", script])

if __name__ == "__main__":
    processes = {}

    # Start all processes initially
    for name, script in SCRIPTS.items():
        processes[name] = start_process(name, script)

    try:
        while True:
            time.sleep(5)  # Check every 5 seconds
            for name, process in processes.items():
                if process.poll() is not None:  # Process ended
                    print(f"{name} process stopped. Restarting...")
                    processes[name] = start_process(name, SCRIPTS[name])
    except KeyboardInterrupt:
        print("Keyboard interrupt received. Shutting down...")
    finally:
        for name, process in processes.items():
            if process.poll() is None:
                print(f"Stopping {name} process...")
                process.terminate()
                process.wait()
        print("All processes stopped. Exiting.")