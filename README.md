# Elsewherr

**What is it?**

Elsewherr is a Python script that connects to your Radarr and Sonarr instances to automatically tag your media based on its availability on streaming services. It uses the TMDb (The Movie Database) API to find out where your movies and shows are streaming.

**How does it work?**

The script fetches your entire media library from Radarr and Sonarr. For each item, it looks up the streaming providers in your specified region (e.g., "US", "CA"). If a movie or show is available on one of your chosen providers (like Netflix or Disney Plus), the script will automatically create and apply a corresponding tag (e.g., `elsewherr-netflix`) in Radarr or Sonarr.

**Why?**

This allows you to see at a glance which of your media is available elsewhere. You can use these tags for various purposes:
* **Library Management:** Create custom filters in Radarr/Sonarr to see what you could potentially remove to save space.
* **Information:** Quickly know if a movie is on a service you subscribe to.
* **Automation:** Use the tags as triggers for other scripts or actions.

---

### **How to Use It**

1.  **Prerequisites:**
    * Python 3.8+
    * A TMDb account with an API key. You can get one for free at [themoviedb.org](https://www.themoviedb.org/).

2.  **Setup:**
    * Download or clone this repository.
    * Install the required Python packages:
        ```bash
        pip install -r requirements.txt
        ```
    * Rename `config.yaml.example` to `config.yaml`.

3.  **Configuration:**
    * Open `config.yaml` and fill in the details as explained in the table below.

4.  **Running the Script:**
    * Run the script from your terminal:
        ```bash
        python elsewherr.py
        ```
    * For more detailed logging, use the `-v` or `--verbose` flag:
        ```bash
        python elsewherr.py -v
        ```

---

### **Configuration (`config.yaml`)**

| Parameter | Description | Example |
| :--- | :--- | :--- |
| **tmdb.api_key** | **Required.** Your API Key for The Movie Database. | `xxx` |
| **tmdb.region** | **Required.** A 2-digit region code to check for streaming availability.<br>Refer /res/regions.txt  | `US` |
| **radarr.enabled** | Set to `true` to enable Radarr processing. | `true` |
| **radarr.url** | The full URL to your Radarr instance, including the port. | `http://localhost:7878` |
| **radarr.api_key**| Your Radarr API key. | `xxx` |
| **sonarr.enabled** | Set to `true` to enable Sonarr processing. | `true` |
| **sonarr.url** | The full URL to your Sonarr instance, including the port. | `http://localhost:8989` |
| **sonarr.api_key**| Your Sonarr API key. | `xxx` |
| **discord.enabled**| Set to `true` to send a summary report to a Discord channel. | `true` |
| **discord.webhook_url**| The webhook URL for your Discord channel. | `xxx` |
| **gotify.enabled** | Set to `true` to enable Gotify notifications. | `false` |
| **gotify.url** | The URL for your Gotify instance. | `http://localhost` |
| **gotify.token** | Your Gotify application token. | `xxx` |
| **providers** | A list of streaming providers you want to track. **Must match TMDb's naming exactly.**<br>Refer /res/providers.txt | `- Netflix` <br> `- Disney Plus` |
| **prefix** | A unique prefix for the tags created by this script. This is crucial for cleanup. | `elsewherr-` |

>[!Important]
>The script uses this **prefix** to identify and remove old tags before adding the new ones. This ensures that media removed from a streaming service will have it's tag correctly removed. **Do not use a generic prefix that might match other tags in your library.**

---

### **Logging & Debugging**

* By default, the script prints INFO-level logs to the console.
* You can enable DEBUG-level logging by running `python elsewherr.py --verbose`.
* To save logs to a file (`elsewherr.log`), run `python elsewherr.py --log-to-file`. The log file is overwritten on each run.