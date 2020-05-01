FROM openjdk:11

ADD build/distributions/executor.tar /opt
RUN ln -s /opt/executor /opt/ballista-jvm

EXPOSE 50051

CMD ["/opt/ballista-jvm/bin/executor"]