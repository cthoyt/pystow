FROM ubuntu:20.04

WORKDIR /sw

# Install and configure neo4j and python environment
RUN apt-get update && \
    apt-get install -y apt-transport-https ca-certificates curl wget software-properties-common && \
    curl -fsSL https://debian.neo4j.com/neotechnology.gpg.key | apt-key add - && \
    add-apt-repository "deb https://debian.neo4j.com stable 4.4" && \
    apt-get install -y neo4j

# Install python
RUN apt-get update && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get install -y git zip unzip bzip2 gcc pkg-config {{ python }}

# See Hynek's post https://hynek.me/articles/docker-uv/
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON={{ python }}

# Install dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system "{{ pip_install }}"

# Ingest graph content into neo4j. Mount the data
RUN \
    {%- for name in edge_names %}
    --mount=type=bind,source={{ name }},target=/sw/{{ name }} \
    {%- endfor %}
    {%- for node_type, name in node_names %}
    --mount=type=bind,source={{ name }},target=/sw/{{ name }} \
    {%- endfor %}
    sed -i 's/#dbms.default_listen_address/dbms.default_listen_address/' /etc/neo4j/neo4j.conf && \
    sed -i 's/#dbms.security.auth_enabled/dbms.security.auth_enabled/' /etc/neo4j/neo4j.conf && \
    neo4j-admin import --delimiter='TAB' --skip-duplicate-nodes=true \
        {%- for name in edge_names %}
        --relationships /sw/{{ name }} \
        {%- endfor %}
        {%- for node_type, name in node_names %}
        --nodes={{ node_type }}=/sw/{{ name }} \
        {%- endfor %}
        --skip-bad-relationships=true

COPY startup.sh startup.sh
ENTRYPOINT ["/bin/bash", "/sw/startup.sh"]
