document.getElementById('demo').innerHTML = "HELLO JS"

class LongPollClient {
  initialize(serverAddress, onMessage) {
    this.serverAddress = serverAddress;
    this.onMessage = onMessage;
    this.sent_messages = 0;
    this.outstandingMessages = new Set();
  }

  request(messages) {
    let messageId = this.sent_messages;
    this.sent_messages += 1;

    this.outstandingMessages.add(messageId);
    makeHTTP(serverAddress, [msg]).then((response) => {
      this.outstandingMessages.delete(messageId);
      for (let receivedMsg of response.messages) {
        this.onMessage(receivedMsg);
      }
      if (x.size == 0) {
        request([]);
      }
    })
  }

  send(msg) {
    this.request([msg]);
  }
}
