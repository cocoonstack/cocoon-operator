FROM scratch

COPY cocoon-operator /cocoon-operator

ENTRYPOINT ["/cocoon-operator"]
