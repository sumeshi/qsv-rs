FROM rust:1.87-slim-bullseye

RUN apt -y update && apt upgrade -qqy && apt -y install \
    curl \
    gcc \
    git \
    fish \
    sudo

ARG USERNAME=dev
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

RUN chsh -s /usr/bin/fish $USERNAME

USER $USERNAME
