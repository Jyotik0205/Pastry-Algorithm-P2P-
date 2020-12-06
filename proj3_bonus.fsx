#if INTERACTIVE
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.Remote.dll"
#r @"bin\MCD\Debug\netcoreapp3.1\Akka.Fsharp.dll"
#endif

open System
open System.Globalization
open System.Threading
open System.Security.Cryptography
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
//Inputs
let numNodes=fsi.CommandLineArgs.[1]|> int;
let numRequests=fsi.CommandLineArgs.[2]|> int;
let perf=fsi.CommandLineArgs.[3]|> int;
let mutable totalhops=0.0
//let numNodes=1000
//let numRequests=1
let mutable bflag=true
let mutable mflag=true
let failrand=System.Random()
let failurenodes=new System.Collections.Generic.List<string>()

let NodesList=new System.Collections.Generic.List<string>()
let rand=System.Random()
type PMessage=
    |Start
    |Build 
    |Converge
    |MDone
    |Show
    |Unrec
type RingMessage=
    |AddtoRing of int*string
type NodeMessage=
    |Add of int*string
    |RouteTable of string[]*int*string
    |PrintRouteTable
    |Forward of string
    |LeafAdd of System.Collections.Generic.List<string>*System.Collections.Generic.List<string>*string
    |Addme of String*string[]
type CMessage=
    |Send
    
let toval (k_hash:string)=
      //  let arr=k_hash.ToCharArray() 
      //             |> Array.map (fun (x : char) -> System.String.Format("{0}"+x.ToString(), "0"))
      //             |> String.concat System.String.Empty  
          let arr="0"+k_hash
          bigint.Parse(arr , System.Globalization.NumberStyles.HexNumber)
      //  bigint.Parse(k_hash , System.Globalization.NumberStyles.HexNumber)

//let y1=toval "0A316E869057A313FE539BF40EA58976 "-toval "0EB7ACF6A2CB859583DCC30B168BE31E" 
let mutable nodesreached=0
// let b=abs(y)>abs(y1)
let prefixlen (s1:String ,s2:String)=
       let mutable flag=true
       let mutable j=0
       let l1=String.length(s1)
       let l2=String.length(s2)
       if s1.Equals(s2) then
        j<-String.length(s1)
       else
           while flag && j<l1&& j<l2 do
              if s1.[j]=s2.[j] then
                   j<-j+1
              else
                   flag<-false
       j
//Hash Generation Using MD5
let ByteToHex bytes = 
    bytes 
    |> Array.map (fun (x : byte) -> System.String.Format("{0:X2}", x))
    |> String.concat System.String.Empty

let generateHash text = 
    let getbytes : (int->byte[]) = System.BitConverter.GetBytes
    let md5=MD5.Create()
    //use algorithm = new System.Security.Cryptography.MD5()
    text |> (getbytes >> md5.ComputeHash>>ByteToHex)

while failurenodes.Count<(numNodes*perf/100) do
    let mutable a=failrand.Next(1,numNodes)
    while failurenodes.Contains(generateHash(a)) do
        a<-failrand.Next(1,numNodes)
    failurenodes.Add(generateHash(a))
let system=ActorSystem.Create("proj3")

let spawn_printer system name n=
  let senderact= spawn system ("Actor"+string(name))<|
                      fun mailbox->
                          let rec loop()=
                                actor{
                                       let! msg=mailbox.Receive()
                                       match msg with
                                       |Send ->       for i=1 to numRequests do
                                                        let mutable a=rand.Next(1,numNodes)
                                                        while a=name do
                                                          a<-rand.Next(1,numNodes)
                                                      //printfn "Number Generated is: %d" a
                                                        let sendnode=system.ActorSelection("akka://proj3/user/parent/"+n)
                                                        sendnode.Tell(Forward (generateHash a))
                                                        Thread.Sleep(1000)
                                       return!loop()
                                }
                          loop()
  spawn system n<|
         fun mailbox->
             let nodeid=(string)n
             let name=name
             let routingTable = Array2D.init 16 32 (fun x y -> ("0"))
             let Leafs1 = Array.init 16 (fun x -> ("0"))
             let Leafsmax=new System.Collections.Generic.List<string>() //Capacity 8
             let Leafsmin=new System.Collections.Generic.List<string>() //Capacity 8
             //Leafs.Add(n)
             let Neighbor=new System.Collections.Generic.List<string>()
             while Neighbor.Count<16 do
               let a=rand.Next(1,numNodes)
               if not(Neighbor.Contains(generateHash a)) && a<>name then
                 Neighbor.Add(generateHash a)
             let mutable lmin:bigint= toval nodeid
             let mutable lmax:bigint= toval nodeid
             let mutable lminv:string= nodeid
             let mutable lmaxv:string= nodeid

             let rec loop()=
                actor{
                   let! msg=mailbox.Receive()
                   match msg with
                   |Add (k,k_hash) ->    let value = toval k_hash
                                         let nval=toval nodeid
                                         let diff=abs(value-toval nodeid)
                                         let len=prefixlen(k_hash,nodeid)
                                         let mutable row=0
                                         let mutable next="0"
                                        
                                         if nodeid.Equals(k_hash) then
                                             printfn "in Node:%d for node:%d WTF" name k
                                             return()
                                      
                                         if value<=lmax && value>=lmin then
                                            let mutable min=toval "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                                            let mutable minleaf="0"
                                            for i=0 to (Leafsmax.Count-1) do
                                              if ((min>abs(value-toval(Leafsmax.[i]))) && (not(Leafsmax.[i].Equals(k_hash)))) then
                                                //printfn "min is changing to=: %A" min
                                                min<-abs(value-toval(Leafsmax.[i])) 
                                                minleaf<-Leafsmax.[i]
                                            for i=0 to (Leafsmin.Count-1) do
                                              if ((min>abs(value-toval(Leafsmin.[i]))) && (not(Leafsmin.[i].Equals(k_hash)))) then
                                                //printfn "min is changing to=: %A" min
                                                min<-abs(value-toval(Leafsmin.[i])) 
                                                minleaf<-Leafsmin.[i]
                                            if min<diff then
                                              next<-minleaf
                                             // printfn "In Leaf checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash
                                        
                                         else 
                                             if len<32 then
                                               row <-Convert.ToInt32(k_hash.[len].ToString(),16)
                                               next<-routingTable.[row,len]
                                               if abs(toval next-nval)>abs(toval next-value) then
                                                  routingTable.[row,len]<-k_hash
                                               if next=k_hash then
                                                  next<-"0"
                                              // printfn "In Route checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash
                                             else
                                               printfn "Something Wrong in 1"
                                               return()
                                             if String.length(next)=1  then
                                                let mutable min=toval "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                                                let mutable minleaf="0"
                                                for i=0 to Leafsmax.Count-1 do
                                                    if ((min>abs(value-toval(Leafsmax.[i]))) && (not(Leafsmax.[i].Equals(k_hash))) && prefixlen(k_hash,Leafsmax.[i])>=len) then
                                                //printfn "min is changing to=: %A" min
                                                       min<-abs(value-toval(Leafsmax.[i])) 
                                                       minleaf<-Leafsmax.[i]
                                                for i=0 to Leafsmin.Count-1 do
                                                    if ((min>abs(value-toval(Leafsmin.[i]))) && (not(Leafsmin.[i].Equals(k_hash))) && prefixlen(k_hash,Leafsmin.[i])>=len) then
                                                //printfn "min is changing to=: %A" min
                                                       min<-abs(value-toval(Leafsmin.[i])) 
                                                       minleaf<-Leafsmin.[i]
                                                for i=0 to (Neighbor.Count-1) do
                                                    if ((min>abs(value-toval(Neighbor.[i]))) && (not(Neighbor.[i].Equals(k_hash))) && prefixlen(k_hash,Neighbor.[i])>=len) && (NodesList.Contains(Neighbor.[i]))then
                                                //printfn "min is changing to=: %A" min
                                                       min<-abs(value-toval(Neighbor.[i])) 
                                                       minleaf<-Neighbor.[i]
                                                for i=0 to 15 do
                                                    for j=0 to 31 do
                                                       if ((min>abs(value-toval(routingTable.[i,j]))) && (not(routingTable.[row,len].Equals(k_hash))) && (prefixlen(k_hash,routingTable.[row,len])>=len)) then
                                                 // printfn "min is changing to in 2nd block=: %A" min
                                                          min<-abs(value-toval(routingTable.[i,j]))
                                                          minleaf<-routingTable.[i,j]
                                                if min<diff then
                                                 next<-minleaf
                                                 //printfn "In All checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash

                                         if String.length(next)=1 then 
                                                 //printfn "Converging here, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash   
                                                 if not(Leafsmax.Contains(k_hash)) && value>nval then                  //Add in Own. This is the converging node
                                                         if Leafsmax.Count<8 then
                                                            Leafsmax.Add(k_hash)
                                                         else
                                                           if value<lmax  then
                                                               Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                               Leafsmax.Add(k_hash)
                                                         Leafsmax.Sort()
                                                         lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                         lmax<-toval lmaxv
                                                 if not(Leafsmin.Contains(k_hash)) && value<nval then                  //Add in Own. This is the converging node
                                                         if Leafsmin.Count<8 then
                                                            Leafsmin.Add(k_hash)
                                                         else
                                                           if value>lmin  then
                                                               Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                               Leafsmin.Add(k_hash)
                                                         Leafsmin.Sort()
                                                         lminv<-Leafsmin.[0]
                                                         lmin<-toval lminv
                                                        
                                                 let parentnode=system.ActorSelection("akka://proj3/user/parent")
                                                 parentnode.Tell(Converge)
                                                 let addnode=system.ActorSelection("akka://proj3/user/parent/"+k_hash)
                                                 if len<32 then
                                                  for i=0 to len do
                                                       let srow=routingTable.[*,i]
                                                       addnode.Tell(RouteTable (srow,i,nodeid))
                                                  addnode.Tell(LeafAdd (new System.Collections.Generic.List<string>(Leafsmax),new System.Collections.Generic.List<string>(Leafsmin),nodeid))
                                         else
                                                 //printfn "%d pos5"name
                                                 let addnode=system.ActorSelection("akka://proj3/user/parent/"+k_hash)
                                                 if len<32 then
                                                  for i=0 to len do
                                                       let srow=routingTable.[*,i]
                                                       addnode.Tell(RouteTable (srow,i,nodeid))
                                                 else 
                                                   printfn "Something wrong in 2"
                                                 if next.Equals(nodeid) then
                                                    printfn "Something Wrong in 3"
                                                 else
                                                     //printfn "In:%s and sending to:%s  and sending:%s and Leaf Count is:%d" nodeid next k_hash Leafs.Count
                                                     let nextnode=system.ActorSelection("akka://proj3/user/parent/"+next)
                                                    // printfn "Sending Message to %s actors from ring" next 
                                                     nextnode.Tell(Add (k,k_hash))

                                         
                                         return ()
                   |RouteTable (srow,li,nodeid1)->     if li<32 then
                                                           srow.[Convert.ToInt32(nodeid1.[li].ToString(),16)]<-nodeid1
                                                           srow.[Convert.ToInt32(nodeid.[li].ToString(),16)]<-"0"
                                                           routingTable.[*,li]<-srow
                                                           
                                                       //printfn "Updated at Actor%d" name
                                                       //printfn "Routing Table for %s is %A" nodeid routingTable
                                                       return()
                   |Addme (nodeid1,srow)     ->        let li=prefixlen(nodeid1,nodeid)
                                                       let valu1=toval nodeid1
                                                       let valu=toval nodeid
                                                       if valu1>valu then
                                                          if not(Leafsmax.Contains(nodeid1)) then
                                                                   if Leafsmax.Count<8 then
                                                                      Leafsmax.Add(nodeid1)
                                                                   else
                                                                     if valu1<lmax then
                                                                          Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                                          Leafsmax.Add(nodeid1)
                                                                          
                                                                   Leafsmax.Sort()
                                                                   lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                                   lmax<-toval lmaxv
                                                       if valu1<valu then
                                                          if not(Leafsmin.Contains(nodeid1)) then
                                                                   if Leafsmin.Count<8 then
                                                                      Leafsmin.Add(nodeid1)
                                                                   else
                                                                     if valu1>lmin then
                                                                          Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                                          Leafsmin.Add(nodeid1)
                                                                          
                                                                   Leafsmin.Sort()
                                                                   lminv<-Leafsmin.[0]
                                                                   lmin<-toval lminv
                                                        
                                                      
                                                       if li<32 then
                                                           for i=0 to 15 do
                                                              if String.length(routingTable.[i,li])=1 && Convert.ToInt32(nodeid.[li].ToString(),16)<>i then 
                                                                 routingTable.[i,li]<-srow.[i]
                                                           for j=0 to 15 do
                                                             if String.length(srow.[j])<>1 then
                                                                let valu1=toval srow.[j]
                                                                   
                                                                if valu1>valu then
                                                                     if not(Leafsmax.Contains(srow.[j])) then
                                                                           if Leafsmax.Count<8 then
                                                                              Leafsmax.Add(srow.[j])
                                                                           else
                                                                             if valu1<lmax then
                                                                                  Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                                                  Leafsmax.Add(srow.[j])
                                                                          
                                                                     Leafsmax.Sort()
                                                                     lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                                     lmax<-toval lmaxv
                                                                if valu1<valu then
                                                                     if not(Leafsmin.Contains(srow.[j])) then
                                                                           if Leafsmin.Count<8 then
                                                                              Leafsmin.Add(srow.[j])
                                                                           else
                                                                             if valu1>lmin then
                                                                                  Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                                                  Leafsmin.Add(srow.[j])
                                                                          
                                                                     Leafsmin.Sort()
                                                                     lminv<-Leafsmin.[0]
                                                                     lmin<-toval lminv
                                                      
                                                       return() 
                   |LeafAdd  (leafmax,leafmin,nodeid1)->          //printfn "Leaf size received:%d" leaf.Count
                                                       let valu1=toval nodeid1
                                                       let valu=toval nodeid
                                                       if leafmax.Remove(nodeid) ||leafmin.Remove(nodeid) then
                                                         if valu1>valu then
                                                          if not(Leafsmax.Contains(nodeid1)) then
                                                                   if Leafsmax.Count<8 then
                                                                      Leafsmax.Add(nodeid1)
                                                                   else
                                                                     if valu1<lmax then
                                                                          Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                                          Leafsmax.Add(nodeid1)
                                                                          
                                                                   Leafsmax.Sort()
                                                                   lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                                   lmax<-toval lmaxv
                                                         if valu1<valu then
                                                          if not(Leafsmin.Contains(nodeid1)) then
                                                                   if Leafsmin.Count<8 then
                                                                      Leafsmin.Add(nodeid1)
                                                                   else
                                                                     if valu1>lmin then
                                                                          Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                                          Leafsmin.Add(nodeid1)
                                                                          
                                                                   Leafsmin.Sort()
                                                                   lminv<-Leafsmin.[0]
                                                                   lmin<-toval lminv
                                                        
                                                      
                                                       for l in leafmax do
                                                        let v=toval l
                                                        if v>valu then
                                                          if not(Leafsmax.Contains(l)) then
                                                                   if Leafsmax.Count<8 then
                                                                      Leafsmax.Add(l)
                                                                   else
                                                                     if v<lmax then
                                                                          Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                                          Leafsmax.Add(l)
                                                                          
                                                                   Leafsmax.Sort()
                                                                   lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                                   lmax<-toval lmaxv
                                                        if v<valu then
                                                          if not(Leafsmin.Contains(l)) then
                                                                   if Leafsmin.Count<8 then
                                                                      Leafsmin.Add(l)
                                                                   else
                                                                     if v>lmin then
                                                                          Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                                          Leafsmin.Add(l)
                                                                          
                                                                   Leafsmin.Sort()
                                                                   lminv<-Leafsmin.[0]
                                                                   lmin<-toval lminv

                                                       for l in leafmin do
                                                        let v=toval l
                                                        if v>valu then
                                                          if not(Leafsmax.Contains(l)) then
                                                                   if Leafsmax.Count<8 then
                                                                      Leafsmax.Add(l)
                                                                   else
                                                                     if v<lmax then
                                                                          Leafsmax.Remove(Leafsmax.[Leafsmax.Count-1])|>ignore
                                                                          Leafsmax.Add(l)
                                                                          
                                                                   Leafsmax.Sort()
                                                                   lmaxv<-Leafsmax.[Leafsmax.Count-1]
                                                                   lmax<-toval lmaxv
                                                        if v<valu then
                                                          if not(Leafsmin.Contains(l)) then
                                                                   if Leafsmin.Count<8 then
                                                                      Leafsmin.Add(l)
                                                                   else
                                                                     if v>lmin then
                                                                          Leafsmin.Remove(Leafsmin.[0])|>ignore
                                                                          Leafsmin.Add(l)
                                                                          
                                                                   Leafsmin.Sort()
                                                                   lminv<-Leafsmin.[0]
                                                                   lmin<-toval lminv
                                                                   
                                                                   
                                                        
                                                       //Leafs<-leaf
                                                       
                                                       //printfn "Leaf after changing:%A for node:%d" Leafs name
                                                     
                                                       for i=0 to 15 do
                                                          for j=0 to 31 do
                                                              if String.length(routingTable.[i,j])<>1 then 
                                                                let addnode=system.ActorSelection("akka://proj3/user/parent/"+routingTable.[i,j]) 
                                                                let h=prefixlen(routingTable.[i,j],nodeid)
                                                                let srow=routingTable.[*,h]
                                                                addnode.Tell(Addme (nodeid,srow))
                                                       for i=0 to (Leafsmax.Count-1) do 
                                                               if String.length(Leafsmax.[i])<>1 && not(Leafsmax.[i].Equals(nodeid)) then 
                                                                let addnode=system.ActorSelection("akka://proj3/user/parent/"+Leafsmax.[i]) 
                                                                let h=prefixlen(Leafsmax.[i],nodeid)
                                                                let srow=routingTable.[*,h]
                                                                addnode.Tell(Addme (nodeid,srow))
                                                       for i=0 to (Leafsmin.Count-1) do 
                                                               if String.length(Leafsmin.[i])<>1 && not(Leafsmin.[i].Equals(nodeid)) then 
                                                                let addnode=system.ActorSelection("akka://proj3/user/parent/"+Leafsmin.[i]) 
                                                                let h=prefixlen(Leafsmin.[i],nodeid)
                                                                let srow=routingTable.[*,h]
                                                                addnode.Tell(Addme (nodeid,srow))
                                                       for i=0 to (Neighbor.Count-1) do 
                                                               if String.length(Neighbor.[i])<>1 && NodesList.Contains(Neighbor.[i]) then 
                                                                let addnode=system.ActorSelection("akka://proj3/user/parent/"+Neighbor.[i]) 
                                                                let h=prefixlen(Neighbor.[i],nodeid)
                                                                let srow=routingTable.[*,h]
                                                                addnode.Tell(Addme (nodeid,srow))
                                                      // printfn "Leaf state is:%d of Actor: %d " Leafs.Count name
                                                       return()

                   |PrintRouteTable->                  printfn "NOde %d" name 
                                                       printfn "Routing Table %A" routingTable 
                                                       printfn "Leafmax:" 
                                                       Leafsmax |> Seq.iter (fun x -> printf "%s " x)
                                                       printfn "Leafmin:" 
                                                       Leafsmin |> Seq.iter (fun x -> printf "%s " x)
                                                       printfn "leafmax:%s and leafmin:%s" lmaxv lminv
                                                       return()

                    |Forward k_hash->    let value = toval k_hash
                                         let nval=toval nodeid
                                         let diff=abs(value-nval)
                                         let len=prefixlen(k_hash,nodeid)
                                         let mutable row=0
                                         let mutable next="0"
                                         let parentnode=system.ActorSelection("akka://proj3/user/parent")
                                         if nodeid.Equals(k_hash) then
                                            // printfn "Done Done"
                                             parentnode.Tell(MDone)
                                             return()
                                         else
                                             if value<=lmax && value>=lmin then
                                                let mutable min=toval "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                                                let mutable minleaf="0"
                                                for i=0 to (Leafsmax.Count-1) do
                                                  if not(failurenodes.Contains(Leafsmax.[i])) then
                                                   if ((min>abs(value-toval(Leafsmax.[i])))) then
                                                    //printfn "min is changing to=: %A" min
                                                    min<-abs(value-toval(Leafsmax.[i])) 
                                                    minleaf<-Leafsmax.[i]
                                                for i=0 to (Leafsmin.Count-1) do
                                                 if not(failurenodes.Contains(Leafsmin.[i])) then
                                                  if ((min>abs(value-toval(Leafsmin.[i])))) then
                                                    //printfn "min is changing to=: %A" min
                                                    min<-abs(value-toval(Leafsmin.[i])) 
                                                    minleaf<-Leafsmin.[i]
                                                if min<diff then
                                                  next<-minleaf
                                                 // printfn "In Leaf checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash
                                              else 
                                                 if len<32 then
                                                   row <-Convert.ToInt32(k_hash.[len].ToString(),16)
                                                   
                                                   if not(failurenodes.Contains(routingTable.[row,len])) then
                                                     next<-routingTable.[row,len]  
                                                  // printfn "In Route checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash
                                                 else
                                                   printfn "Something Wrong in 1"
                                                   return()
                                                 if String.length(next)=1 then
                                                    let mutable min=toval "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
                                                    let mutable minleaf="0"
                                                    for i=0 to Leafsmax.Count-1 do
                                                       if not(failurenodes.Contains(Leafsmax.[i])) then
                                                        if ((min>abs(value-toval(Leafsmax.[i]))) && (not(Leafsmax.[i].Equals(k_hash))) && prefixlen(k_hash,Leafsmax.[i])>=len) then
                                                    //printfn "min is changing to=: %A" min
                                                           min<-abs(value-toval(Leafsmax.[i])) 
                                                           minleaf<-Leafsmax.[i]
                                                    for i=0 to Leafsmin.Count-1 do
                                                      if not(failurenodes.Contains(Leafsmin.[i])) then
                                                        if ((min>abs(value-toval(Leafsmin.[i]))) && (not(Leafsmin.[i].Equals(k_hash))) && prefixlen(k_hash,Leafsmin.[i])>=len) then
                                                    //printfn "min is changing to=: %A" min
                                                           min<-abs(value-toval(Leafsmin.[i])) 
                                                           minleaf<-Leafsmin.[i]
                                                    for i=0 to (Neighbor.Count-1) do
                                                      if not(failurenodes.Contains(Neighbor.[i])) then
                                                        if ((min>abs(value-toval(Neighbor.[i]))) && (not(Neighbor.[i].Equals(k_hash))) && prefixlen(k_hash,Neighbor.[i])>=len) && (NodesList.Contains(Neighbor.[i]))then
                                                    //printfn "min is changing to=: %A" min
                                                           min<-abs(value-toval(Neighbor.[i])) 
                                                           minleaf<-Neighbor.[i]
                                                    for i=0 to 15 do
                                                        for j=0 to 31 do
                                                          if not(failurenodes.Contains(routingTable.[i,j])) then
                                                           if ((min>abs(value-toval(routingTable.[i,j]))) && (not(routingTable.[row,len].Equals(k_hash))) && (prefixlen(k_hash,routingTable.[row,len])>=len)) then
                                                     // printfn "min is changing to in 2nd block=: %A" min
                                                              min<-abs(value-toval(routingTable.[i,j]))
                                                              minleaf<-routingTable.[i,j]
                                                    if min<diff then
                                                     next<-minleaf
                                                     //printfn "In All checj, In node:%d with hash:%s and forwarding to:%s for new node:%s  " name n next k_hash

                                             if String.length(next)=1 then 
                                                    //printfn "Doesnt exists in node:%s and looking for:%s" nodeid k_hash
                                                    //let selfact=system.ActorSelection("akka://proj3/user/parent/"+nodeid)
                                                    //selfact.Tell(PrintRouteTable)
                                                    let y=prefixlen(k_hash,nodeid)
                                                    routingTable.[Convert.ToInt32(k_hash.[y].ToString(),16),y]<-k_hash
                                                    let selfact1=system.ActorSelection("akka://proj3/user/parent")
                                                    selfact1.Tell(Unrec)
                                                    let elfact=system.ActorSelection("akka://proj3/user/parent/"+k_hash)
                                                    elfact.Tell(Forward k_hash)
                                                    totalhops<-totalhops+1.0
                                             else
                                                if next.Equals(nodeid) then
                                                  printfn "Something Wrong"
                                            
                                                else
                                                 //printfn "In:%s and sending to:%s pos5 and sending:%s" nodeid next k_hash
                                                 let nextnode=system.ActorSelection("akka://proj3/user/parent/"+next)
                                                 //printfn "Sending Message to %s actors from %s" next nodeid
                                                // if (int)totalhops%100=0 then
                                                   // printfn "Hoped:%f" totalhops
                                                 totalhops<-totalhops+1.0
                                                 nextnode.Tell(Forward k_hash)
                                                    
                                         return ()
                   return! loop() 
                }
             loop()
let ring= spawn system "Pastry"<|fun mailbox->
  
  let mutable pointofcontact=""
  let rec loop()=
                actor{
                   let! msg=mailbox.Receive()
                   match msg with
                   |AddtoRing (k,k_hash) ->  if NodesList.Count=0 then
                                              NodesList.Add(k_hash)
                                              pointofcontact<-k_hash
                                              let par=system.ActorSelection("akka://proj3/user/parent")
                                              par.Tell(Converge)
                                              //printfn "NodeList Size %d and poiint of contact:%s " NodesList.Count pointofcontact
                                             else
                                              pointofcontact<-NodesList.[rand.Next(0,NodesList.Count-1)]
                                              NodesList.Add(k_hash)
                                              //printfn "NodeList Size %d and poiint of contact:%s " NodesList.Count pointofcontact
                                              
                                              let pointOfContact=system.ActorSelection("akka://proj3/user/parent/"+string(pointofcontact))
                                              //printfn "Sending Message to %s actors from ring" pointofcontact 
                                              pointOfContact.Tell(Add (k,k_hash))
                                              pointofcontact<-k_hash
                                //printf "Sent to parent"
                   return! loop() 
                }
  loop()
let controllerActor= spawn system "parent" <| fun mailbox->
 
               
               let mutable nodessettled=0
               //let mutable nodesreached=0
               let mutable currnode=0
               let mutable unrec=0
               let ActorHashList =  
                   [1..numNodes]
                      |> List.map(fun id-> generateHash(id))
               let ActorList =  
                   [1..numNodes]
                      |> List.map(fun id-> spawn_printer mailbox id ActorHashList.[id-1])
              
               let rec loop()=
                   actor{
                   let! msg=mailbox.Receive()
                  // printfn "From Parent %A" msg
                   match msg with
                   |Start->       //printfn "Received Message"
                                  for i=1 to numNodes do
                                     if not(failurenodes.Contains(generateHash(i))) then
                                      let childsender=system.ActorSelection("akka://proj3/user/parent/"+"Actor"+string(i))
                                      childsender.Tell(Send)
                                      Thread.Sleep(10)
                                  return()
                           
                   
                   |Show  ->    //printfn "Received Message at show" 
                                for i=1 to numNodes do
                                      let childsender=system.ActorSelection("akka://proj3/user/parent/"+ActorHashList.[i-1])
                                      childsender.Tell(PrintRouteTable)
                                      Thread.Sleep(1000)
                                return()
                   |Build->      let ring=system.ActorSelection("akka://proj3/user/Pastry")
                                // printfn "Sending Message to all actors: List:%A" ActorHashList
                                 currnode<-currnode+1
                                 //for i=1 to numNodes do
                                // printfn "Sending Message to Ring for %d actor" currnode
                                 ring.Tell(AddtoRing (currnode,ActorHashList.[currnode-1]))
                                     // Thread.Sleep(50)
                                 return()
                   |Converge ->  nodessettled<-nodessettled+1
                                 //printfn "Settled:%d" nodessettled
                                 if nodessettled=numNodes then
                                    //printfn "Now settled"
                                    bflag<-false
                                 if currnode<numNodes then
                                  let ring=system.ActorSelection("akka://proj3/user/Pastry")
                                // printfn "Sending Message to all actors: List:%A" ActorHashList
                                  currnode<-currnode+1
                                 //for i=1 to numNodes do
                                      //printfn "Sending Message to %d actor" i
                                  Thread.Sleep(10)
                                  ring.Tell(AddtoRing (currnode,ActorHashList.[currnode-1]))
                                 
                   |MDone ->   nodesreached<-nodesreached+1  
                              // if nodesreached%10=0 then
                               //printfn "NOdes Reached: %d" nodesreached
                               if nodesreached=(numNodes-numNodes*perf/100)*numRequests then
                                 mflag<-false
                                 printfn "NOdes Reached"
                                 printfn "Not Reached:%d" unrec
                   |Unrec->   unrec<-unrec+1
                               
                   return! loop()
                    

                   }
               loop()

let parent=system.ActorSelection("akka://proj3/user/parent")
parent.Tell(Build)
printfn "Building topology using Network Join...."
while bflag do
  Async.Sleep(100)|>ignore
// let act=system.ActorSelection("akka://proj3/user/parent/"+"0CD2C8FF6A0F6182FE4715BFCCACC9B9 ")
// act.Tell(PrintRouteTable)
Thread.Sleep(1000)
// let act1=system.ActorSelection("akka://proj3/user/parent/"+"740BAB4F9EC8808AEDB68D6B1281AEB2  ")
// act1.Tell(PrintRouteTable)
//Console.ReadLine()
//parent.Tell(Show)
//Console.ReadLine()
let parent1=system.ActorSelection("akka://proj3/user/parent")
//Console.ReadLine()
//parent1.Tell(Show)
//Console.ReadLine()
parent1.Tell(Start)
printfn "Sending message using Pastry routing algorithm...."
let mutable prevnode=0
let mutable times=0
while mflag do
  Async.Sleep(100)|>ignore
  if prevnode<nodesreached then
     prevnode<-nodesreached
     times<-0
  else
    times<-times+1
    if times=1000000000 then
      printfn "Stuck at :%d"nodesreached 
     // mflag<-false

let avg=totalhops/((float)(numNodes-numNodes*perf/100)*(float)numRequests)
printfn "Average Hops: %f" avg
//parent.Tell(Start)
//Console.ReadLine()