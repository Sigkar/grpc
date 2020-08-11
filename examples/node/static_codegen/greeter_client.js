/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

const messages = require("./helloworld_pb");
const services = require("./helloworld_grpc_pb");

const grpc = require("grpc");

const main = () => {
  const client = new services.GreeterClient(
    "localhost:50051",
    grpc.credentials.createInsecure()
  );
  const request = new messages.HelloRequest();
  let user;
  if (process.argv.length >= 3) {
    user = process.argv[2];
  } else {
    user = "world";
  }
  request.setName(user);
  client.sayHello(request, (err, response) => {
    console.log("Greeting:", response.getMessage());
  });
};

main();
