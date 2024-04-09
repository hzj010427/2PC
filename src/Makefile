JAVAC = javac

SOURCES = $(wildcard *.java)

CLASSES = $(SOURCES:.java=.class)

all: $(CLASSES)

%.class: %.java
	$(JAVAC) $<

clean:
	rm -f *.class
