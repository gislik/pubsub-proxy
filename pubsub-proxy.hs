{-# LANGUAGE OverloadedStrings #-}

module Main where

import Data.Maybe               (fromMaybe)
import Data.ByteString          (ByteString)
import Data.ByteString.Char8    (unpack)
import Control.Monad            (forever)
import Control.Applicative      (liftA)
import System.Environment       (getEnvironment)
import System.Log.Logger
import System.ZMQ3.Monadic

--------------------------------------------------------------------------------
-- zeromq
--------------------------------------------------------------------------------
type Dsn = String

logDsn :: Dsn
logDsn = "inproc://logger"

initSubscriber :: Dsn -> ZMQ z (Socket z XSub)
initSubscriber dsn = do
   s <- socket XSub
   bind s dsn
   return s

initPublisher :: Dsn -> ZMQ z (Socket z XPub)
initPublisher dsn = do
   s <- socket XPub
   bind s dsn
   return s

initCapturer :: ZMQ z (Socket z Pair)
initCapturer = do
   s <- socket Pair
   connect s logDsn
   return s

initLogger :: ZMQ z (Socket z Pair)
initLogger = do
   s <- socket Pair
   bind s logDsn
   return s

receiveFirst :: Receiver a => Socket z a -> ZMQ z ByteString
receiveFirst s = liftA head (receiveMulti s)

--------------------------------------------------------------------------------
-- logger
--------------------------------------------------------------------------------
logger :: ZMQ z ()
logger =  do
   s <- initLogger
   forever $ receiveFirst s >>= liftIO . infoM "pubsub-proxy.logger" . unpack

--------------------------------------------------------------------------------
-- main
--------------------------------------------------------------------------------
main :: IO ()
main = do 
   updateGlobalLogger rootLoggerName (setLevel INFO)
   infoM "pubsub-proxy.main" "Starting program"
   env <- getEnvironment
   let pdsn = fromMaybe "tcp://127.0.0.1:7006" $ lookup "ZMQPUBLISHER"  env
   let sdsn = fromMaybe "tcp://127.0.0.1:7004" $ lookup "ZMQSUBSCRIBER" env
   infoM "pubsub-proxy.main" "Starting logger"
   runZMQ $ do
      liftIO $ debugM "pubsub-proxy.main" "Configuring sockets"
      pub <- initPublisher  pdsn
      sub <- initSubscriber sdsn
      -- capturer <- initCapturer
      liftIO $ infoM "pubsub-proxy.main" "PubSub proxy"
      proxy pub sub Nothing -- (Just capturer) 
