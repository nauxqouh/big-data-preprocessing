# [Personal-Note]

1. `ARG BUILDARCH` in Dockerfile?

    ```bash
    ARG BUILDARCH
    ```

    `ARG` = **Build-time argument**, it only exists in builg image period, it means when you `docker build`.

    `BUILDARCH`: determine CPU structure for your environment, example:
    - `amd64` (Intel/AMD)
    - `arm64` (Apple M1/M2)

    this bash help Dockerfile selecting right URL or setting files for your current structure.

2. Different from `apk` and `apt`

    - Use `apk` if you want a lightweight, fast, and space-optimized Docker image.
    - Use `apt` (Advance Package Tool) if you need a near real machine environment (Ubuntu/Debian).

3. `openjdk8-jdk`

4. `EXPOSE` and `not EXPOSE` port?

    `EXPOSE`: assign port that container using.

5. `CMD`