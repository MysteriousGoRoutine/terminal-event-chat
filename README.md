# terminal-event-chat

CLI chat written in Go: users exchange messages via Kafka, communicate in chat rooms, and a bot handles commands.

MVP:
* The user starts the program with a nickname;
* enters a message in the terminal;
* the message is sent to the Kafka topic chat.messages;
* another running client receives and displays it.


## Next Step
Значит producer и consumer работают параллельно.

Next MVP step: remove the three fixed messages and read text from the terminal using `bufio.Scanner`. Keep the consumer in a goroutine, and send each entered line to Kafka.
