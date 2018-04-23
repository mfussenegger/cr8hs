{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Concurrent.Async (async, wait)
import           Control.Monad            (unless, replicateM)
import           Data.Aeson               ((.:), (.=))
import qualified Data.Aeson               as A
import qualified Data.Aeson.Types         as A
import qualified Data.ByteString          as B
import qualified Data.ByteString.Lazy     as BL
import           Data.Semigroup           ((<>))
import qualified Data.Text                as T
import           GHC.Generics
import           Network.HTTP.Client      (Manager, newManager)
import qualified Network.HTTP.Client      as NC
import           Network.HTTP.Client.TLS  (tlsManagerSettings)
import           Options.Applicative
import           Pipes
import qualified Pipes.Concurrent         as P
import           System.IO                (isEOF)


data Args = Args
  { hosts :: String
  , concurrency :: Int }
  deriving (Show)


parser :: ParserInfo Args
parser = parserInfo
  where
    parserInfo = info (argsDef <**> helper)
      ( fullDesc
      <> progDesc "Execute queries received on stdin" )
    argsDef :: Parser Args
    argsDef = Args
      <$> strOption
        ( long "hosts"
        <> metavar "HOSTS"
        <> showDefault
        <> value "localhost:4200" )
      <*> option auto
        ( long "concurrency" 
        <> metavar "CONCURRENCY"
        <> showDefault
        <> value 1 )


fromStdin :: Producer BL.ByteString IO ()
fromStdin = do
  eof <- lift isEOF
  unless eof $ do
    line <- lift $ BL.fromStrict <$> B.getLine
    if line == "QUIT"
      then pure ()
      else do
        yield line
        fromStdin


data Query = Query
  { stmt :: T.Text
  , args :: [A.Value]
  , mode :: T.Text }
  deriving (Generic, Show)

instance A.FromJSON Query


parseQuery :: Monad m => Pipe BL.ByteString Query m ()
parseQuery = do
  line <- await
  case (A.eitherDecode line :: Either String Query) of
    Left msg    -> error $ "Invalid query input: " <> msg
    Right query -> do
      yield query
      parseQuery


execQuery :: String -> Manager -> Pipe Query Double IO ()
execQuery host manager = do
  query <- await
  respBody <- lift $ sendQuery query
  case getDuration respBody of
    Nothing -> execQuery host manager
    Just duration -> do
      yield duration
      execQuery host manager
  where
    getDuration :: BL.ByteString -> Maybe Double
    getDuration resp = A.decode resp >>= A.parseMaybe (.: "duration")
    sendQuery query = do
      initReq <- NC.parseRequest ("http://" <> host <> "/_sql")
      let
        reqObj = case mode query of
          "single" -> A.object [ "stmt" .= stmt query
                               , "args" .= args query ]
          "bulk"   -> A.object [ "stmt" .= stmt query
                               , "bulk_args" .= args query ]
          m        -> error $ "Invalid query mode" <> show m
        body = NC.RequestBodyBS (BL.toStrict $ A.encode reqObj)
        req = initReq 
          { NC.method = "POST"
          , NC.requestBody = body
          , NC.requestHeaders = [ ("Content-Type", "application/json") ]
          }
      NC.responseBody <$> NC.httpLbs req manager


printResult :: Consumer Double IO ()
printResult = do
  result <- await
  lift $ print result


main :: IO ()
main = do
  args <- execParser parser
  manager <- newManager tlsManagerSettings
  let
    queries :: Producer Query IO ()
    queries = fromStdin >-> parseQuery
    runQueries = execQuery (hosts args) manager
    processQueries = for runQueries (lift . print)
  (output, input) <- P.spawn (P.bounded (concurrency args * 2))
  workers <- replicateM (concurrency args) $
    async $ do runEffect $ P.fromInput input >-> processQueries
               P.performGC
  producer <- async $ do runEffect $ queries >-> P.toOutput output
                         P.performGC
  mapM_ wait (producer:workers)
