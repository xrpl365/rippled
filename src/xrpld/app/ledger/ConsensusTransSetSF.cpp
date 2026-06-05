#include <xrpld/app/ledger/ConsensusTransSetSF.h>

#include <xrpld/app/ledger/TransactionMaster.h>
#include <xrpld/app/misc/Transaction.h>

#include <xrpl/basics/Blob.h>
#include <xrpl/basics/Log.h>
#include <xrpl/basics/SHAMapHash.h>
#include <xrpl/beast/utility/instrumentation.h>
#include <xrpl/core/Job.h>
#include <xrpl/core/JobQueue.h>
#include <xrpl/protocol/HashPrefix.h>
#include <xrpl/protocol/Serializer.h>
#include <xrpl/protocol/digest.h>  // IWYU pragma: keep
#include <xrpl/server/NetworkOPs.h>
#include <xrpl/shamap/SHAMapTreeNode.h>

#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace xrpl {

ConsensusTransSetSF::ConsensusTransSetSF(Application& app, NodeCache& nodeCache)
    : app_(app), nodeCache_(nodeCache), j_(app.getJournal("TransactionAcquire"))
{
}

void
ConsensusTransSetSF::gotNode(
    bool fromFilter,
    SHAMapHash const& nodeHash,
    std::uint32_t,
    Blob&& nodeData,  // NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
    SHAMapNodeType type) const
{
    if (fromFilter)
        return;

    if ((type == SHAMapNodeType::TnTransactionNm) && (nodeData.size() > 16))
    {
        // this is a transaction, and we didn't have it
        JLOG(j_.debug()) << "Node on our acquiring TX set is TXN we may not have";

        try
        {
            // skip prefix
            Serializer const s(nodeData.data() + 4, nodeData.size() - 4);
            SerialIter sit(s.slice());
            auto stx = std::make_shared<STTx const>(std::ref(sit));
            XRPL_ASSERT(
                stx->getTransactionID() == nodeHash.asUInt256(),
                "xrpl::ConsensusTransSetSF::gotNode : transaction hash "
                "match");

            // Rather than caching the (potentially large) transaction blob in
            // nodeCache_, store the transaction in the TransactionMaster so
            // getNode() can rebuild the leaf on demand. This is done
            // synchronously to close the window before the asynchronous
            // submission below populates the cache.
            std::string reason;
            auto tx = std::make_shared<Transaction>(stx, reason, app_);
            app_.getMasterTransaction().canonicalize(&tx);

            auto const pap = &app_;
            app_.getJobQueue().addJob(
                JtTransaction, "TxsToTxn", [pap, stx]() { pap->getOPs().submitTransaction(stx); });
            return;
        }
        catch (std::exception const& ex)
        {
            JLOG(j_.warn()) << "Fetched invalid transaction in proposed set. Exception: "
                            << ex.what();
        }
    }

    // Inner nodes have no alternative recovery source, so they must be cached
    // for getNode() to satisfy later lookups. Any leaf we could not store in
    // the TransactionMaster above also falls through to here.
    nodeCache_.insert(nodeHash, nodeData);
}

std::optional<Blob>
ConsensusTransSetSF::getNode(SHAMapHash const& nodeHash) const
{
    Blob nodeData;
    if (nodeCache_.retrieve(nodeHash, nodeData))
        return nodeData;

    auto txn = app_.getMasterTransaction().fetchFromCache(nodeHash.asUInt256());

    if (txn)
    {
        // this is a transaction, and we have it
        JLOG(j_.trace()) << "Node in our acquiring TX set is TXN we have";
        Serializer s;
        s.add32(HashPrefix::TransactionId);
        txn->getSTransaction()->add(s);
        XRPL_ASSERT(
            sha512Half(s.slice()) == nodeHash.asUInt256(),
            "xrpl::ConsensusTransSetSF::getNode : transaction hash match");
        nodeData = s.peekData();
        return nodeData;
    }

    return std::nullopt;
}

}  // namespace xrpl
