# See here for image contents: https://github.com/microsoft/vscode-dev-containers/tree/v0.238.0/containers/python-3/.devcontainer/base.Dockerfile

# [Choice] Python version (use -bullseye variants on local arm64/Apple Silicon): 3, 3.10, 3.9, 3.8, 3.7, 3.6, 3-bullseye, 3.10-bullseye, 3.9-bullseye, 3.8-bullseye, 3.7-bullseye, 3.6-bullseye, 3-buster, 3.10-buster, 3.9-buster, 3.8-buster, 3.7-buster, 3.6-buster
ARG VARIANT="3.10-bullseye"
FROM mcr.microsoft.com/vscode/devcontainers/python:0-${VARIANT}

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends gpg

# Configure pyenv
USER vscode
RUN curl https://pyenv.run | bash
RUN echo 'export PYENV_ROOT="/workspaces/dbx/.venv"' >> $HOME/.bashrc
RUN echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
RUN echo eval "$($HOME/.pyenv/bin/pyenv init -)" >> $HOME/.bashrc
RUN . ${HOME}/.bashrc \
   && $HOME/.pyenv/bin/pyenv install 3.8.13 \
   && $HOME/.pyenv/bin/pyenv global 3.8.13
