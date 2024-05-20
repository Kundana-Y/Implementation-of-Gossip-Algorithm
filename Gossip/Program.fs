open System
open Akka.FSharp
// Select a random element from the given list
let randomSelector (l: List<_>) =
    // Create a random number
    let randomNumber = Random()
    // The random number is used to select an element from the list
    l.[randomNumber.Next(l.Length)]

// Rounds the number of nodes based on the mentioned topology 
let roundingNodes nodes topology =
    // Match the specified topology
    match topology with
    | "imp3d" -> Math.Pow (Math.Round ((float nodes) ** (1.0 / 3.0)), 3.0)  |> int
    | "2d"
    | _ -> nodes

// Fetches a random neighbor from the topology mapping for a given node ID
let fetchRandomNeighbor (topologyMapping: Map<_, _>) nodeID =
    // Retrieve the list of neighbors for the specified node ID from the topology mapping
    let (neighbors: List<_>) = (topologyMapping.TryFind nodeID).Value
    let random = Random()
    // The random number is used to select a neighbor ID from the list of neighbors
    neighbors.[random.Next(neighbors.Length)]

// Full topology function mapping for the specified number of nodes
let fullTopologyFunction nodes =
    let mutable map = Map.empty
    [ 1 .. nodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = List.filter (fun y -> nodeID <> y) [ 1 .. nodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    // Return the final topology mapping
    map


// 2D grid mapping for the specified node ID
let grid2D nodeID nodes =
    let mutable map = Map.empty
    // Calculating the length of side for 2D grid
    let sideLength = sqrt (float nodes) |> int
    // Filter nodes based on the 2D grid rules
    [ 1 .. nodes ]
    |> List.filter (fun y ->
        if (nodeID % sideLength = 0) then (y = nodeID - 1 || y = nodeID - sideLength || y = nodeID + sideLength)
        elif (nodeID % sideLength = 1) then (y = nodeID + 1 || y = nodeID - sideLength || y = nodeID + sideLength)
        else (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - sideLength || y = nodeID + sideLength))

// 2D topology function mapping for the specified number of nodes
let twoDimTopologyFunction nodes =
    let mutable map = Map.empty
    [ 1 .. nodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = grid2D nodeID nodes
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    // Return the final topology mapping
    map

// 3D grid mapping for the specified node ID
let grid3D nodeID nodes =
    // Calculating the length of side for 3D grid
    let sideLength = Math.Round(Math.Pow((float nodes), (1.0 / 3.0))) |> int
    // Filter nodes based on the 3D grid rules
    [ 1 .. nodes ]
    |> List.filter (fun y ->
        if (nodeID % sideLength = 0) then
            if (nodeID % (int (float (sideLength) ** 2.0)) = 0) then
                (y = nodeID - 1 || y = nodeID - sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
            elif (nodeID % (int (float (sideLength) ** 2.0)) = sideLength) then
                (y = nodeID - 1 || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
            else
                (y = nodeID - 1 || y = nodeID - sideLength || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))        
        elif (nodeID % sideLength = 1) then
            if (nodeID % (int (float (sideLength) ** 2.0)) = 1) then
                (y = nodeID + 1 || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
            elif (nodeID % (int (float (sideLength) ** 2.0)) = int (float (sideLength) ** 2.0) - sideLength + 1 ) then
                (y = nodeID + 1 || y = nodeID - sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
            else
                (y = nodeID + 1 || y = nodeID - sideLength || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
        elif (nodeID % (int (float (sideLength) ** 2.0)) > 1) && (nodeID % (int (float (sideLength) ** 2.0)) < sideLength) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
        elif (nodeID % (int (float (sideLength) ** 2.0)) > int (float (sideLength) ** 2.0) - sideLength + 1) && (nodeID % (int (float (sideLength) ** 2.0)) < (int (float (sideLength) ** 2.0))) then
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0)))
        else
            (y = nodeID - 1 || y = nodeID + 1 || y = nodeID - sideLength || y = nodeID + sideLength || y = nodeID - int ((float (sideLength) ** 2.0)) || y = nodeID + int ((float (sideLength) ** 2.0))))


// 3D topology function mapping for the specified number of nodes with random neighbor
let threeDimTopologyFunction nodes =
    let mutable map = Map.empty
    [ 1 .. nodes ]
    |> List.map (fun nodeID ->
        let mutable listNeighbors = grid3D nodeID nodes
        let random =
            [ 1 .. nodes ]
            |> List.filter (fun m -> m <> nodeID && not (listNeighbors |> List.contains m))
            |> randomSelector
        let listNeighbors = random :: listNeighbors
        // Add the node ID and its final list of neighbors to the topology mapping
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    // Return the final topology mapping
    map

// Line topology function mapping for the specified number of nodes
let lineTopologyFunction nodes =
    let mutable map = Map.empty
    [ 1 .. nodes ]
    |> List.map (fun nodeID ->
        let listNeighbors = List.filter (fun y -> (y = nodeID + 1 || y = nodeID - 1)) [ 1 .. nodes ]
        map <- map.Add(nodeID, listNeighbors))
    |> ignore
    // Return the final topology mapping
    map

// Topology function mapping for the specified number of nodes
let topologyFunction nodes topology =
    let mutable map = Map.empty
    // Match the specified topology type and call the corresponding topology function
    match topology with
    | "line" -> lineTopologyFunction nodes
    | "2d" -> twoDimTopologyFunction nodes
    | "imp3d" -> threeDimTopologyFunction nodes
    | "full" -> fullTopologyFunction nodes

// Message representation used for communication between nodes in both the algorithms
type CounterMessage =
    | GossipNodeConverge
    | PushSumNodeConverge of int * float

// Result of the node convergence and elapsed time
type Result = { NodesConverged: int; ElapsedTime: int64; }

// Counter for tracking gossip and push sum convergence and elapsed time
let counter initialCount nodes (filepath: string) (stWatch: Diagnostics.Stopwatch) (mailbox: Actor<'a>) =
    let rec loop count (dataframeList: Result list) =
        actor {
            let! message = mailbox.Receive()
            match message with
            | GossipNodeConverge ->
                let newRecord = { NodesConverged = count + 1; ElapsedTime = stWatch.ElapsedMilliseconds; }
                if (count + 1 = nodes) then
                    stWatch.Stop()
                    printfn "Convergence Time (Gossip algorithm) %d ms" stWatch.ElapsedMilliseconds
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
            | PushSumNodeConverge (nodeID, avg) ->
                let newRecord = { NodesConverged = count + 1; ElapsedTime = stWatch.ElapsedMilliseconds }
                if (count + 1 = nodes) then
                    stWatch.Stop()
                    printfn "Convergence Time (Push Sum algorithm) %d ms" stWatch.ElapsedMilliseconds
                    mailbox.Context.System.Terminate() |> ignore
                return! loop (count + 1) (List.append dataframeList [newRecord])
        }
    loop initialCount []

// Representation of gossip algorithm actor
let gossipalgorithm maxCount (topologyMapping: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop (count: int) = actor {
        let! message = mailbox.Receive ()
        // Match the received message and perform necessary action
        match message with
        | "heardRumor" ->
            if count = 0 then
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                
                counterRef <! GossipNodeConverge
                return! loop (count + 1)
            else
                return! loop (count + 1)
        | "spreadRumor" ->
            if count >= maxCount then
                return! loop count
            else
                let neighborID = fetchRandomNeighbor topologyMapping nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                neighborRef <! "heardRumor"
                mailbox.Context.System.Scheduler.ScheduleTellOnce(
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    "spreadRumor"
                )
                return! loop count
        | _ ->
            // Log an error message for unhandled messages
            printfn "Node %d has received unhandled message" nodeID
            return! loop count
    }
    loop 0


type PushsumMsg =
    | Initialize
    | Message of float * float
    | Round

// Representation of Push Sum algorithm actor
let pushsumalgorithm (topologyMapping: Map<_, _>) nodeID counterRef (mailbox: Actor<_>) = 
    let rec loop sNode wNode sSum wSum count isTransmitting = actor {
        if isTransmitting then
            let! message = mailbox.Receive ()
            // Match the received message and perform necessary action
            match message with
            | Initialize ->
                mailbox.Self <! Message (float nodeID, 1.0)
                mailbox.Context.System.Scheduler.ScheduleTellRepeatedly (
                    TimeSpan.FromMilliseconds(0.0),
                    TimeSpan.FromMilliseconds(25.0),
                    mailbox.Self,
                    Round
                )
                return! loop (float nodeID) 1.0 0.0 0.0 0 isTransmitting
            | Message (s, w) ->
                return! loop sNode wNode (sSum + s) (wSum + w) count isTransmitting
            | Round ->
                let neighborID = fetchRandomNeighbor topologyMapping nodeID
                let neighborPath = @"akka://my-system/user/worker" + string neighborID
                let neighborRef = mailbox.Context.ActorSelection(neighborPath)
                mailbox.Self <! Message (sSum / 2.0, wSum / 2.0)
                neighborRef <! Message (sSum / 2.0, wSum / 2.0)
                if(abs ((sSum / wSum) - (sNode / wNode)) < 1.0e-10) then
                    let newCount = count + 1
                    if newCount = 10 then
                        counterRef <! PushSumNodeConverge (nodeID, sSum / wSum)
                        return! loop sSum wSum 0.0 0.0 newCount false
                    else
                        return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 newCount isTransmitting 
                else
                    return! loop (sSum / 2.0) (wSum / 2.0) 0.0 0.0 0 isTransmitting
    }
    loop (float nodeID) 1.0 0.0 0.0 0 true

// Entry point of the program
[<EntryPoint>]
let main argv =
    let system = System.create "my-system" (Configuration.load())

    // Define the maximum number of gossip or push sum rounds
    let maxCount = 10

    // Command line arguments extraction
    let topology = argv.[1]
    let nodes = roundingNodes (int argv.[0]) topology
    let algorithm = argv.[2]
    let filepath = "results/" + topology + "-" + string nodes + "-" + algorithm + ".csv"
    
    // Build the topology mapping based on the specific topology
    let topologyMapping = topologyFunction nodes topology

    let stWatch = Diagnostics.Stopwatch()

    let counterRef = spawn system "counter" (counter 0 nodes filepath stWatch)

    // Match the selected algorithm and perform the appropriate action
    match algorithm with
    | "gossip" ->
        let workerRef =
            [ 1 .. nodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                spawn system name (gossipalgorithm maxCount topologyMapping nodeID counterRef))
            |> randomSelector
        
        stWatch.Start()
     
        workerRef <! "heardRumor"

    | "pushsum" ->
        let workerRef =
            [ 1 .. nodes ]
            |> List.map (fun nodeID ->
                let name = "worker" + string nodeID
                (spawn system name (pushsumalgorithm topologyMapping nodeID counterRef)))
     
        stWatch.Start()
     
        workerRef |> List.iter (fun item -> item <! Initialize)


    system.WhenTerminated.Wait()
    // Return the exit code
    0 